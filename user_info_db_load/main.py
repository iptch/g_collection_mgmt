import pandas as pd
from sqlalchemy import create_engine
import os


def read_gsheet_csv(path):
    return pd.read_csv(path, header=0)


def read_employee_csv(path):
    return pd.read_csv(path, header=0)


def adjust_content(df_answers, df_employees):
    df_answers.drop('Zeitstempel', axis=1, inplace=True)
    df_answers.rename(columns={'Mein ipt Kürzel lautet:': 'acronym',
                               'Welcher ipt Team Member Kategorie gehöre ich an?': 'team',
                               'Als Kind wollte ich immer ... werden? (halte dich kurz)': 'job',
                               'Was ist meine Superpower? (halte dich kurz)': 'superpower',
                               'Was gefällt mir an ipt besonders gut? (halte dich kurz)': 'highlight',
                               'Ohne ... könnte ich niemals leben (halte dich kurz):': 'must_have'},
                      inplace=True)
    df_answers['acronym'] = df_answers['acronym'].apply(str.upper)

    df_employees.rename(columns={'Kürzel': 'acronym', 'Name': 'name'}, inplace=True)

    df_merged = pd.merge(df_answers, df_employees, how='right', on='acronym')
    df_merged['image_link'] = df_merged['acronym'].apply(lambda x: f'{x.lower()}.jpg')
    return df_merged.dropna()


def write_to_postgres(df):
    host = 'g-collection-postgres.postgres.database.azure.com'
    port = '5432'
    database = 'g-collection-db'
    username = 'gcollectionadmin'
    password = os.environ['POSTGRES_PW']
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')
    df.to_sql('core_card', engine, if_exists='replace', index=True, index_label='id')


def write_to_sqlite(df):
    # good for debugging locally to have the same cards
    engine = create_engine(f'sqlite:///../../g_collection_be/db.sqlite3')
    df.to_sql('core_card', engine, if_exists='replace', index=True, index_label='id')


def main():
    df_answers = read_gsheet_csv('data/google_forms_answers.csv')
    df_employees = read_employee_csv('data/ma_2023.csv')

    df_answers_adjusted = adjust_content(df_answers, df_employees)
    write_to_postgres(df_answers_adjusted)
    write_to_sqlite(df_answers_adjusted)


if __name__ == "__main__":
    main()
