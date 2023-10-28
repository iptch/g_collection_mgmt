import pandas as pd
from sqlalchemy import create_engine
import os
import re


def read_gsheet_csv(path):
    df_answers = pd.read_csv(path, header=0)

    # rename columns
    df_answers.rename(columns={'E-Mail-Adresse': 'email',
                               'Ich heisse (Vorname und Nachname)': 'name',
                               'Mein Kürzel lautet': 'acronym',
                               'Aktuell bin ich in folgender Rolle tätig:': 'job',
                               'Ich bin an folgendem Datum bei ipt gestartet': 'start_at_ipt',
                               'Wo würdest Du am liebsten einmal hinreisen? ': 'wish_destination',
                               'Wenn Du mit jemandem für einen Tag das Leben tauschen könntest, wer würde das sein? ': 'wish_person',
                               'Welchen Skill oder welches Talent wolltest Du schon immer einmal erlernen?': 'wish_skill',
                               'Was ist der beste berufliche Ratschlag, den Du je erhalten hast?': 'best_advice'},
                      inplace=True)

    # ensure that full email is used
    check_emails(df_answers)

    # df_answers['email_clean'] = df_answers.apply(clean_email, axis=1)
    # select only newest entry

    return df_answers

def check_emails(df_answers):
    emails = df_answers['email']
    unclean_emails = emails[emails.str.match(r'^(\w{3})@ipt.ch$')]
    if unclean_emails.any():
        print('E-Mails with acronym found. Update GSheet to use full name email.')
        print(unclean_emails)
        raise Exception('E-Mails with acronym found. Update GSheet to use full name email.')
    return
def clean_email(row):
    email_raw = row['email_raw']

    regex_pattern = re.compile(r'^(\w{3})@ipt.ch$')
    if regex_pattern.match(email_raw):
        print(f'Short email found, adjust it in Google Sheet! - {email_raw}')
    return 'abc@ipt.ch'


def adjust_content(df_answers):
    # df_answers.drop('Zeitstempel', axis=1, inplace=True)
    df_answers.rename(columns={'Mein ipt Kürzel lautet:': 'acronym',
                               'Welcher ipt Team Member Kategorie gehöre ich an?': 'team',
                               'Als Kind wollte ich immer ... werden? (halte dich kurz)': 'job',
                               'Was ist meine Superpower? (halte dich kurz)': 'superpower',
                               'Was gefällt mir an ipt besonders gut? (halte dich kurz)': 'highlight',
                               'Ohne ... könnte ich niemals leben (halte dich kurz):': 'must_have'},
                      inplace=True)
    df_answers['acronym'] = df_answers['acronym'].apply(str.upper)

    return df_answers


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

    df_answers_adjusted = adjust_content(df_answers)
    # write_to_postgres(df_answers_adjusted)
    # write_to_sqlite(df_answers_adjusted)


if __name__ == "__main__":
    main()
