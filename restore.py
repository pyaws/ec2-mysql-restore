import os
import sys
import json
import zipfile
import urllib.request
import tempfile
import boto3

if __name__ == '__main__':
    import sys
    import json
    import os

    args = sys.argv
    if len(args) >= 2:
        stage = args[1]
        config_filename = 'config.' + stage + '.json'
        parent_dir = os.path.dirname(os.path.abspath(__file__))
        config_filepath = os.path.join(parent_dir, config_filename)
    else:
        print("Stage option not specified.")
        sys.exit(0)

    with open(config_filepath, 'r') as fp:
        config = json.load(fp)

    region = config['REGION']

    rds_client = boto3.client('rds-data')
    try:
        db_arn = config["AURORA_DB_ARN"]
        db_secret_arn = config["AURORA_DB_SECRET_ARN"]
        db_name = config["AURORA_DB_NAME"]
        db_source_url = config["DB_SOURCE_URL"]
    except KeyError:
        print("""Missing key-val pairs AURORA_DB_ARN, AURORA_DB_SECRET_ARN and/or AURORA_DB_NAME in {CONFIG}
                Unable to create SQL Tables""".format(CONFIG=config_filename))
        sys.exit(0)



    print(region)
    print(db_arn)
    print(db_secret_arn)
    print(db_name)
    print(db_source_url)

    download_path = "/tmp/download.zip"
    print("Downloading archive from {}".format(db_source_url))
    ret = urllib.request.urlretrieve(db_source_url, download_path)
    print("Downloading finished")

    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)

        with zipfile.ZipFile(download_path, 'r') as archive:
            archive.extractall()
        files_path = tmpdir + '/db_24_0_mysql'
        files = os.listdir(files_path)
        files.remove('Read Me.txt')
        files.sort()

        print(files)
        sys.exit(0)
        for item in files:
            table_name = item[3:-4]
            print('Starting transaction for {}'.format(table_name))
            response = rds_client.execute_statement(secretArn=db_secret_arn, database=db_name, resourceArn=db_arn,
                                                    sql="SET FOREIGN_KEY_CHECKS = 0;")

            response = rds_client.execute_statement(
                secretArn=db_secret_arn,
                database=db_name,
                resourceArn=db_arn,
                sql="DROP TABLE IF EXISTS {TABLE_NAME};".format(TABLE_NAME=table_name)
            )

            response = rds_client.execute_statement(secretArn=db_secret_arn, database=db_name, resourceArn=db_arn,
                                                    sql="SET FOREIGN_KEY_CHECKS = 1;")

            file_path = files_path + '/' + item
            file = open(file_path, 'r')
            file_lines = file.readlines()

            sql_statement = ''
            for line in file_lines:
                line = line.replace('\n', '')

                # escape blank lines
                if not line:
                    continue

                sql_statement += line
                # get full sql statement
                if line[-1] != ';':
                    continue

                # ignore semicolons in the statement
                sql_statement = sql_statement.replace(';', '')
                sql_statement += ';'

                response = rds_client.execute_statement(
                    secretArn=db_secret_arn,
                    database=db_name,
                    resourceArn=db_arn,
                    sql=sql_statement
                )
                sql_statement = ''

            print('Finished transaction for {}'.format(table_name))

        os.remove(download_path)
        print('Completed.')