import argparse
import os
import pandas as pd
import datetime


schema = [{'name': 'id', 'type': 'INTEGER'}, {'name': 'date', 'type': 'DATETIME'},
          {'name': 'from', 'type': 'DATE'}, {'name': 'to', 'type': 'DATE'},
          {'name': 'count_rows', 'type': 'INTEGER'}, {'name': 'table', 'type': 'STRING'}]


def read_table_from_appsflyer(api_token, app_id, table, start_date, end_date):
    url = 'https://hq.appsflyer.com/export/{3}/{4}/v5?api_token={2}&from={0}&to={1}'.format(start_date, end_date,
                                                                                            api_token, app_id, table)
    df = pd.read_csv(url)
    df.columns = df.columns.str.replace('/', '_')
    df.columns = df.columns.str.replace('(', '')
    df.columns = df.columns.str.replace(')', '')
    df.columns = df.columns.str.replace(' ', '_')
    return df


def write_table_into_bigquery(project_id, dataset_id, table, df, res_df, oauth_key):
    if table in ['partners_by_date_report', 'geo_by_date_report']:
        date_df = df.drop_duplicates(subset=['Date'])
        for date in date_df['Date'].tolist():
            write_df = df.loc[df['Date'] == date]
            write_df.to_gbq(destination_table='{0}.{1}_{2}'.format(dataset_id, table, date.replace('-', '')),
                            project_id=project_id,
                            if_exists='replace', private_key=oauth_key)
    elif table in ['installs_report', 'uninstall_events_report']:
        dates = []
        for time in df['Event_Time'].tolist():
            dates.append(time.split(' ')[0])
        dates = set(dates)
        for date in dates:
            write_df = df[df.Event_Time.str.contains(date + '.*')]
            write_df.to_gbq(destination_table='{0}.{1}_{2}'.format(dataset_id, table, date.replace('-', '')),
                            project_id=project_id,
                            if_exists='replace', private_key=oauth_key)
    else:
        df.to_gbq(destination_table='{0}.{1}'.format(dataset_id, table), project_id=project_id,
                  if_exists='append', private_key=oauth_key)
    res_df.to_gbq(destination_table='{0}.import_appsflyer_log'.format(dataset_id), project_id=project_id,
                  if_exists='append',
                  private_key=oauth_key, table_schema=schema)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--project',
                        required=True,
                        help='BigQuery project id')
    parser.add_argument('-d', '--dataset',
                        required=True,
                        help='BiqQuery dataset id')
    parser.add_argument('-i', '--app_id',
                        required=True,
                        help='Appsflyer app id')
    parser.add_argument('-t', '--api_token',
                        required=True,
                        help='Appsflyer api token')
    parser.add_argument('-T', '--tables',
                        required=True,
                        help='List of tables loaded from Appsflyer into BigQuery')
    parser.add_argument('-a', '--oauth_file',
                        dest='oauth_file',
                        # required=True,
                        help='File to authorize BigQuery')
    args = parser.parse_args(argv)

    if args.oauth_file is not None:
        OAUTH_PATH = args.oauth_file
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = OAUTH_PATH
        oauth_file = open(OAUTH_PATH, 'r')
        oauth_key = oauth_file.read()
    else:
        oauth_key = None

    app_id = args.app_id
    api_token = args.api_token
    project_id = args.project
    dataset_id = args.dataset
    tables = args.tables.split(',')

    for table in tables:
        full_loaded = pd.read_gbq(
            query='SELECT * FROM `{0}.{1}.import_appsflyer_log`'.format(project_id, dataset_id, table),
            private_key=oauth_key, dialect='standard')
        loaded = pd.read_gbq(
            query='SELECT * FROM `{0}.{1}.import_appsflyer_log` WHERE table=\'{2}\''.format(project_id, dataset_id,
                                                                                            table),
            private_key=oauth_key, dialect='standard')

        if loaded.empty:
            id = 0
            if full_loaded.empty:
                id = 1
            else:
                full_loaded = full_loaded.sort_values(by=['id'])
                id = full_loaded.at[full_loaded.index[-1], 'id'] + 1

            run_date = datetime.datetime.now()
            from_date = datetime.date.today() - datetime.timedelta(days=30)
            to_date = datetime.date.today() - datetime.timedelta(days=1)

            partners_by_date_df = read_table_from_appsflyer(app_id=app_id, api_token=api_token, table=table,
                                                            start_date=from_date.strftime('%Y-%m-%d'),
                                                            end_date=to_date.strftime('%Y-%m-%d'))
            print 'Table: {0} was downloaded from appsflyer. Start date: {1}, end date: {2}'.format(table,
                                                                                                    from_date.strftime('%Y-%m-%d'),
                                                                                                    to_date.strftime('%Y-%m-%d'))
            count_rows = partners_by_date_df.shape[0]
            loaded_df = pd.DataFrame(data=[[id, run_date, from_date, to_date, count_rows, table]],
                                     columns=['id', 'date', 'from',
                                              'to', 'count_rows', 'table'])
            write_table_into_bigquery(project_id=project_id, dataset_id=dataset_id, table=table, df=partners_by_date_df,
                                      res_df=loaded_df, oauth_key=oauth_key)

            print 'Table: {0} was uploaded to bigquery'.format(table)
        else:
            full_loaded = full_loaded.sort_values(by=['id'])
            loaded = loaded.sort_values(by=['id'])
            id = full_loaded.at[full_loaded.index[-1], 'id'] + 1
            run_date = datetime.datetime.now()
            from_date = loaded.at[loaded.index[-1], 'to'] + datetime.timedelta(days=1)
            to_date = datetime.date.today() - datetime.timedelta(days=1)
            if from_date <= to_date:
                table_df = read_table_from_appsflyer(app_id=app_id, api_token=api_token, table=table,
                                                     start_date=from_date.strftime('%Y-%m-%d'),
                                                     end_date=to_date.strftime('%Y-%m-%d'))
                print 'Table: {0} was downloaded from Appsflyer. Start date: {1}, end date: {2}'.format(table,
                                                                                                        from_date.strftime('%Y-%m-%d'),
                                                                                                        to_date.strftime('%Y-%m-%d'))
                count_rows = table_df.shape[0]
                loaded_df = pd.DataFrame(data=[[id, run_date, from_date, to_date, count_rows, table]],
                                         columns=['id', 'date',
                                                  'from', 'to',
                                                  'count_rows', 'table'])
                write_table_into_bigquery(project_id=project_id, dataset_id=dataset_id, table=table, df=table_df,
                                          res_df=loaded_df, oauth_key=oauth_key)
                print 'Table: {0} was uploaded to BigQuery'.format(table)


if __name__ == '__main__':
    main()
