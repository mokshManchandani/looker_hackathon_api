from flask import Flask,send_from_directory
from flask_cors import CORS
from scripts import github_downloader,utils,lkml_parser,download_sys_activity,download_user_with_attributes,bigquery_service
import shutil
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes by default

@app.route('/')
def index():    

    # fetch sys activity data from looker instance for a particular dashboard
    g_downloader = github_downloader.GitDownloader(repo_link="https://github.com/mokshManchandani/looker-course.git",local_dir='repo_data')
    g_downloader.download_content()

    l_parser = lkml_parser.LKMLParser(view_file_glob=g_downloader.view_file_glob,model_file_glob=g_downloader.model_file_glob)
    l_parser.read_view_files()
    l_parser.get_model_name()
    l_parser.sanitize_content()
    l_parser.create_df()
    
    s_downloader = download_sys_activity.SysActivityDownloader()
    s_downloader.fetch_dashboard_list(l_parser.model_name)
    s_downloader.fetch_data()
    s_downloader.create_df()

    merged_df = utils.merge_dataframes(l_parser.view_level_df,s_downloader.sys_activity_df)
    
    users_downloader = download_user_with_attributes.UserAttributeDownloader()
    users_df = users_downloader.dump_users()

    merged_df.to_parquet("temp/merged_data.parquet",index=False)
    users_df.to_parquet("temp/user_content.parquet",index=False)

    mapping = {
        "user_content_test":"temp/user_content.parquet",
        "merged":"temp/merged_data.parquet"
    }
    bq_service = bigquery_service.BigqueryService(name_mapping=mapping)
    bq_service.create_tables()
    shutil.rmtree('temp')

    return send_from_directory('static','index.html')

if __name__ == '__main__':
    app.run(debug=True,port=3001)
