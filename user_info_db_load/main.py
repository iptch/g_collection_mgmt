import pandas as pd
from sqlalchemy import create_engine
import os


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

    # ensure that full email is used
    check_emails(df_answers)

    # select only newest entry
    df_answers = df_answers.sort_values('Zeitstempel').groupby('email').tail(1)
    df_answers.reset_index(drop=True, inplace=True)

    df_answers['acronym'] = df_answers['acronym'].apply(str.upper)
    df_answers['start_at_ipt'] = pd.to_datetime(df_answers['start_at_ipt'], format='%d.%m.%Y').dt.date

    # drop not needed columns
    df_answers.drop('Zeitstempel', axis=1, inplace=True)
    # df_answers.drop('email', axis=1, inplace=True)

    return df_answers


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
    df_answers = read_gsheet_csv('data/data_20231028.csv')

    # write_to_postgres(df_answers)
    write_to_sqlite(df_answers)


if __name__ == "__main__":
    main()
