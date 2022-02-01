# Activity report of apache/spark repository
#
# This program collects data on the following metrics:
#   commits
#   issue creation
#   pull requests
#   branches
#   contributions


import json
import requests
import numpy as np
import os
import pandas as pd
from IPython import get_ipython
from pandas import json_normalize
from sqlalchemy import create_engine, engine, text, types, MetaData, Table, String

#TODO: add set df
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

#TODO: add github credentials
github_username = "machavez257"
github_token = "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"

#TODO: create API session. provide input parameters
github_api = "https://api.github.com"
gh_session = requests.Session()
gh_session.auth = (github_username, github_token)
#TODO: add function to convert all object columns to strings, in order to store them efficiently into db
def objects_to_strings(table):
    measurer = np.vectorize(len)
    df_object = table.select_dtypes(include=[object])
    string_columns = dict(zip(df_object, measurer(
        df_object.values.astype(str)).max(axis=0)))
    string_columns = {key: String(length=value) if value > 0 else String(length=1)
                      for key, value in string_columns.items()}
    return string_columns
#TODO: data extraction function -
#   Note: Github API call returns only 30 results. must figure out how to increase number of returned results
#   @params: owner, repo name
#   @returns a list of dictionaries
def commits_of_repo_github(repo, owner, api):
    commits = []
    next = True
    i = 1
    while next == True:
        url = api + '/repos/{}/{}/commits?page={}&per_page=100'.format(owner, repo, i)
        commit_pg = gh_session.get(url = url)
        commit_pg_list = [dict(item, **{'repo_name':'{}'.format(repo)}) for item in commit_pg.json()]
        commit_pg_list = [dict(item, **{'owner':'{}'.format(owner)}) for item in commit_pg_list]
        commits = commits + commit_pg_list
        if 'Link' in commit_pg.headers:
            if 'rel="next"' not in commit_pg.headers['Link']:
                next = False
        i = i + 1
    return commits
#TODO: add function that generates pandas df from list of dictionaries - Convert JSON to table
# @params owner, repo name
# @returns dataframe
def create_commits_df(repo, owner, api):
    commits_list = commits_of_repo_github(repo, owner, api)
    return json_normalize(commits_list)

#TODO: invoke function & add
#commits = create_commits_df('spark', 'apache', github_api)
#TODO: save table to csv file
#commits.to_csv('commits.csv')


#issue creation
def issues_of_repo(repo, owner, api):
    issues = []
    next = True
    i = 1
    while next == True:
        url = api + '/repos/{}/{}/issues?page={}&per_page=100'.format(owner, repo, i)
        issue_pg = gh_session.get(url = url)
        issue_pg_list = [dict(item, **{'repo_name':'{}'.format(repo)}) for item in issue_pg.json()]
        issue_pg_list = [dict(item, **{'owner':'{}'.format(owner)}) for item in issue_pg_list]
        issues = issues + issue_pg_list
        if 'Link' in issue_pg.headers:
            if 'rel="next"' not in issue_pg.headers['Link']:
                next = False
        i = i + 1
    return issues



#pull requests
def pulls_of_repo(repo, owner, api):
    pulls = []
    next = True
    i = 1
    while next == True:
        url = api + '/repos/{}/{}/pulls?page={}&per_page=100'.format(owner, repo, i)
        pull_pg = gh_session.get(url = url)
        pull_pg_list = [dict(item, **{'repo_name':'{}'.format(repo)}) for item in pull_pg.json()]
        pull_pg_list = [dict(item, **{'owner':'{}'.format(owner)}) for item in pull_pg_list]
        pulls = pulls + pull_pg_list
        if 'Link' in pull_pg.headers:
            if 'rel="next"' not in pull_pg.headers['Link']:
                next = False
        i = i + 1
    return pulls


#   branches
def branches_of_repo(repo, owner, api):
    branches = []
    next = True
    i = 1
    while next == True:
        url = api + '/repos/{}/{}/branches?page={}&per_page=100'.format(owner, repo, i)
        branch_pg = gh_session.get(url = url)
        branch_pg_list = [dict(item, **{'repo_name':'{}'.format(repo)}) for item in branch_pg.json()]
        branch_pg_list = [dict(item, **{'owner':'{}'.format(owner)}) for item in branch_pg_list]
        branches = branches + branch_pg_list
        if 'Link' in branch_pg.headers:
            if 'rel="next"' not in branch_pg.headers['Link']:
                next = False
        i = i + 1
    return branches


#GENERATE ALL DATA
def generate_repo_data(repo, owner, api):
    print('COMMIT DATA')
    commits = create_commits_df(repo, owner, api)
    commits.to_csv('commits.csv')
    print('COMMIT DATA COMPLETED')

    print('ISSUE DATA')
    issues = json_normalize(issues_of_repo(repo, owner, api))
    issues.to_csv('issues.csv')
    print('ISSUE DATA COMPLETED')

    print('branches DATA')
    branches = json_normalize(branches_of_repo('spark', 'apache', github_api))
    branches.to_csv('branches.csv')
    print('branches DATA COMPLETED')

    print('PULL REQUEST DATA')
    pulls = json_normalize(pulls_of_repo(repo, owner, api))
    pulls.to_csv('pulls.csv')
    print('PULL REQUEST DATA COMPLETED')

generate_repo_data('spark', 'apache', github_api)
