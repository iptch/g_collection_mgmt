import pandas as pd
import sqlalchemy
import os

import uuid


def read_gsheet_csv(path):
    df_answers = pd.read_csv(path, header=0)

    # rename columns
    df_answers.rename(columns={'E-Mail-Adresse': 'email',
                               'Ich heisse (Vorname und Nachname)': 'name',
                               'Mein Kürzel lautet': 'acronym',
                               'Aktuell bin ich in folgender Rolle tätig:': 'job',
                               'Ich bin an folgendem Datum bei ipt gestartet': 'start_at_ipt',
                               'Wo würdest Du am liebsten einmal hinreisen? ': 'wish_destination',
                               'Wenn Du mit jemandem für einen Tag das Leben tauschen könntest, wer würde das sein?': 'wish_person',
                               'Welchen Skill oder welches Talent wolltest Du schon immer einmal erlernen?': 'wish_skill',
                               'Was ist der beste berufliche Ratschlag, den Du je erhalten hast?': 'best_advice'},
                      inplace=True)

    # remove trailing whitespaces in all columns
    df_answers = df_answers.map(lambda x: x.strip() if isinstance(x, str) else x)

    check_emails(df_answers)
    check_acronyms(df_answers)

    # select only newest entry
    df_answers = df_answers.sort_values('Zeitstempel').groupby('acronym').tail(1)
    df_answers.reset_index(drop=True, inplace=True)

    df_answers['start_at_ipt'] = pd.to_datetime(df_answers['start_at_ipt'], format='%d.%m.%Y').dt.date

    # drop not needed columns
    df_answers.drop('Zeitstempel', axis=1, inplace=True)

    return df_answers

def check_acronyms(df_answers):
    df_answers['acronym'] = df_answers['acronym'].apply(str.upper)
    incorrect_acronyms = df_answers['acronym'][~df_answers['acronym'].str.match(r'^(\w{3})$')]

    if incorrect_acronyms.any():
        print('Incorrect acronyms found:')
        print(incorrect_acronyms)
        raise Exception('Incorrect acronyms found.')
    return

def check_emails(df_answers):
    emails = df_answers['email']
    acronym_emails = emails[emails.str.match(r'^(\w{3})@ipt\.ch$')]
    not_ipt_emails = emails[~emails.str.contains('@ipt.ch')]

    if acronym_emails.any():
        print('E-Mails with acronym found. Update GSheet to use full name email.')
        print(acronym_emails)
        raise Exception('E-Mails with acronym found. Update GSheet to use full name email.')
    if not_ipt_emails.any():
        print('E-Mails with non-ipt domain found found. Update GSheet with ipt mail.')
        print(not_ipt_emails)
        raise Exception('E-Mails with non-ipt domain found found. Update GSheet with ipt mail.')
    return


def upsert_df(df: pd.DataFrame, table_name: str, engine: sqlalchemy.engine.Engine):
    """Implements the equivalent of pd.DataFrame.to_sql(..., if_exists='update')
    (which does not exist). Creates or updates the db records based on the
    dataframe records.
    Conflicts to determine update are based on the dataframes index.
    This will set primary keys on the table equal to the index names
    1. Create a temp table from the dataframe
    2. Insert/update from temp table into table_name
    Returns: True if successful
    """
    # Based on this gist, but needed to update it for sqlalchemy 2.0:
    # https://gist.github.com/pedrovgp/b46773a1240165bf2b1448b3f70bed32

    with engine.connect() as con:
        if not con.execute(sqlalchemy.sql.text(
                f"""SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = 'public'
                AND    table_name   = '{table_name}');
                """
        )).first()[0]:
            raise Exception(f'Table {table_name} does not exist. Create it first with "python manage.py migrate"')

    # If it already exists...
    temp_table_name = f"temp_{uuid.uuid4().hex[:6]}"
    df.to_sql(temp_table_name, engine, index=True)

    index = list(df.index.names)
    index_sql_txt = ", ".join([f'"{i}"' for i in index])
    columns = list(df.columns)
    headers = index + columns
    headers_sql_txt = ", ".join(
        [f'"{i}"' for i in headers]
    )  # index1, index2, ..., column 1, col2, ...

    # col1 = exluded.col1, col2=excluded.col2
    update_column_stmt = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])

    # Compose and execute upsert query
    query_upsert = f"""
    INSERT INTO "{table_name}" ({headers_sql_txt}) 
    SELECT {headers_sql_txt} FROM "{temp_table_name}"
    ON CONFLICT ({index_sql_txt}) DO UPDATE 
    SET {update_column_stmt};
    """

    with engine.connect() as con:
        con.execute(sqlalchemy.sql.text(query_upsert))
        con.execute(sqlalchemy.sql.text(f'DROP TABLE "{temp_table_name}"'))
        con.commit()
    return True


def write_to_postgres(df):
    host = 'g-collection-postgres.postgres.database.azure.com'
    port = '5432'
    database = 'g-collection-db'
    username = 'gcollectionadmin'
    password = os.environ['POSTGRES_PW']
    engine = sqlalchemy.create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

    table_name = 'core_card'
    df_old = pd.read_sql(f'SELECT id, acronym FROM {table_name}', con=engine)
    df_adjusted_index = increasing_ffill_index(df_old, df)
    upsert_df(df=df_adjusted_index, table_name=table_name, engine=engine)


def write_to_sqlite(df):
    engine = sqlalchemy.create_engine(f'sqlite:///../g_collection_be/db.sqlite3')
    table_name = 'core_card'
    df_old = pd.read_sql(f'SELECT id, acronym FROM {table_name}', con=engine)
    df_adjusted_index = increasing_ffill_index(df_old, df)
    with engine.connect() as con:
        con.execute(sqlalchemy.sql.text(f'DELETE FROM {table_name}'))
        con.commit()
    df_adjusted_index.to_sql(table_name, engine, if_exists='append', index=True, index_label='id')


def increasing_ffill_index(df_old, df_new):
    """
    We want to keep the old ids of the card and assign new ids to new cards
    """
    df_new_adjusted = df_new.merge(df_old, on='acronym', how='left')
    max_old_id = df_new_adjusted['id'].max()
    if pd.isnull(max_old_id):
        counter = 0
    else:
        counter = max_old_id + 1
    for index, row in df_new_adjusted.iterrows():
        if pd.isna(row['id']):
            df_new_adjusted.at[index, 'id'] = counter
            counter += 1
    df_new_adjusted = df_new_adjusted.astype({'id': 'int'})
    df_new_adjusted.set_index('id', inplace=True)

    return df_new_adjusted


def main():
    df_answers = read_gsheet_csv('user_info_db_load/data/data_20231101.csv')

    write_to_postgres(df_answers)
    # write_to_sqlite(df_answers)


if __name__ == "__main__":
    main()
