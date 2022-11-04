import json
import configparser
import pickle
import time
import traceback
from datetime import datetime
import pygsheets
import requests
import re
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient import discovery
from gevent import os, config

config = configparser.ConfigParser()
config.read('config.ini')


def google_auth_for_fair_sheet():
    SCOPES = [
        'https://www.googleapis.com/auth/drive.readonly',
        'https://www.googleapis.com/auth/spreadsheets.readonly'
    ]

    credentials = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            credentials = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                config['GOOGLE']['credentials_filename'], SCOPES)
            credentials = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(credentials, token)
    service = discovery.build('sheets', 'v4', credentials=credentials)
    return service


def guid_collection(googlesheet_url):
    # This extracts Google sheet ID from Google sheet url. ID is necessary for authorization
    spreadsheet_id = googlesheet_url.split('https://docs.google.com/spreadsheets/d/', 1)[1].split('/edit', 1)[0]
    ranges = ['B2:B4000', 'I2:I4000']  # collects dataset ID and evaluation result data
    value_render_option = 'FORMATTED_VALUE'
    date_time_render_option = 'FORMATTED_STRING'
    request = google_auth_for_fair_sheet().spreadsheets().values().batchGet(spreadsheetId=spreadsheet_id, ranges=ranges,
                                                                            valueRenderOption=value_render_option,
                                                                            dateTimeRenderOption=date_time_render_option)
    response = request.execute()
    doi_list = response['valueRanges'][0]['values']
    if 'values' in response['valueRanges'][1]:
        result_list = response['valueRanges'][1]['values']
    else:
        result_list = []  # no results present
    doi_len = len(doi_list)
    result_len = len(result_list)

    candidate_for_processing = {}
    for i in range(0, doi_len):
        if doi_list[i]:
            # check if result cell is missing output
            if i >= result_len or result_list[i] == []:
                # check if doi is not empty
                candidate_for_processing[str(i+2)] = ''.join(doi_list[i])
    return candidate_for_processing


def push_to_fair_evaluator(evaluator_url, datasetID, use_datacite):
    metadata = {"metadata_service_endpoint": "",
                "metadata_service_type": "",
                "object_identifier": datasetID,
                "test_debug": True,
                "use_datacite": use_datacite
                }
    auth = (config['EVALUATOR']['evaluator_user'], config['EVALUATOR']['evaluator_password'])
    headers = {"Content-Type": "application/json"}
    evaluation_result = requests.post(evaluator_url, data=json.dumps(metadata), headers=headers, auth=auth, timeout=300)
    if evaluation_result.status_code == 200:
        print(evaluation_result.text)
    else:
        print('Connection error code: %s' % evaluation_result.status_code)

    print(evaluation_result.text)
    evaluation_result = json.loads(evaluation_result.text)
    return evaluation_result


def calculate_score(evaluation_result):
    print("RESULT:", evaluation_result)
    f_score = 0
    a_score = 0
    i_score = 0
    r_score = 0
    f_score_max = 0
    a_score_max = 0
    i_score_max = 0
    r_score_max = 0
    total_tests = 0
    scores_dict = {}
    result_string_f = []
    result_string_a = []
    result_string_i = []
    result_string_r = []
    result_string = ''
    status_code = ''
    success_string = []
    retrieving_page_list = []
    pid = 'PID not extracted'
    pid_type = 'No PID type'
    result_string_f.append("'")  # google sheet will treat the cell value as string

    pid_prefixes = {'doi': 'http://doi.org/', 'handle': 'http://hdl.handle.net/', 'urn': 'http://nbn-resolving.org/'}

    def list_success_identifiers():
        for element in id_score['test_debug']:
            if 'SUCCESS' in element:
                success_identifier = '(' + str(id_score['metric_identifier']) + ') ' + element[9:]  # remove SUCCESS
                if success_identifier not in success_string:  # already existent identifier is not added
                    success_string.append(success_identifier)
        return success_string

    def update_score(score, score_max, id_score, result_string):
        score += id_score['score']['earned']  # Sum score
        score_max += id_score['score']['total']  # Sum max possible score
        if id_score['test_status'] == 'pass':
            result_string.append('1')
        else:
            result_string.append('0')
        return score, score_max

    def search_retrieving_url(string):
        # findall() has been used
        # with valid conditions for urls in string
        regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
        url = re.findall(regex, string)
        return [x[0] for x in url]

    for id_score in evaluation_result["results"]:
        total_tests += 1
        metric_identifier = id_score['metric_identifier']

        if metric_identifier == 'FsF-F1-02D':
            if id_score['output']['pid_scheme']:  # if PID type exists
                pid_type = '(PID type): ' + id_score['output']['pid_scheme']
                if id_score['output']['pid_scheme'] not in pid_prefixes:
                    pid_type = '(PID type): WARNING! ' + id_score['output']['pid_scheme']
            if id_score['output']['pid']:  # if PID exists
                print(id_score['output']['pid'])
                print(id_score['output']['pid_scheme'])
                print(pid_prefixes)
                schema = eval(id_score['output']['pid_scheme'])[0]
                pid = '(PID extracted): ' + id_score['output']['pid'].replace(pid_prefixes[schema], '')
            for element in id_score['test_debug']:
                print(element)
                if 'Retrieving page' in element:
                    retrieving_page_url = ''.join(search_retrieving_url(element))
                    retrieving_page_list.append(retrieving_page_url)
                if 'status code' in element:
                    status_code = ''.join(re.findall(r'\d+', element))
                    print(status_code)

        if metric_identifier.startswith('FsF-F'):
            f_score, f_score_max = update_score(f_score, f_score_max, id_score, result_string_f)
        list_success_identifiers()

        if metric_identifier.startswith('FsF-A'):
            a_score, a_score_max = update_score(a_score, a_score_max, id_score, result_string_a)
        list_success_identifiers()

        if metric_identifier.startswith('FsF-I'):
            i_score, i_score_max = update_score(i_score, i_score_max, id_score, result_string_i)
        list_success_identifiers()

        if metric_identifier.startswith('FsF-R'):
            r_score, r_score_max = update_score(r_score, r_score_max, id_score, result_string_r)
        list_success_identifiers()
    print(status_code)
    print(f_score)
    print(a_score)
    print(i_score)
    print(r_score)


    print(retrieving_page_list)
    test_result_list = result_string_f + result_string_a + result_string_i + result_string_r  # join all sub-lists
    result_string = result_string.join(test_result_list)  # covert list to string
    total_score = f_score + a_score + i_score + r_score
    max_score = f_score_max + a_score_max + i_score_max + r_score_max
    scores_dict.update({'datasetID': evaluation_result['request']['object_identifier']})
    scores_dict.update({'F score': str(round((f_score/f_score_max*100), 2)) + '%'})
    scores_dict.update({'A score': str(round((a_score/a_score_max*100), 2)) + '%'})
    scores_dict.update({'I score': str(round((i_score/i_score_max*100), 2)) + '%'})
    scores_dict.update({'R score': str(round((r_score/r_score_max*100), 2)) + '%'})
    scores_dict.update({'Total score': str(total_score)})
    scores_dict.update({'Total points': str(max_score)})
    scores_dict.update({'Total score percent': str(round((total_score/max_score*100), 2)) + '%'})
    scores_dict.update({'Result string': result_string})
    scores_dict.update({'Success string': ', '.join(success_string)})
    scores_dict.update({'Retrieving pages': retrieving_page_list})
    scores_dict.update({'Status code of FsF-F1-02D': status_code})
    scores_dict.update({'doi': pid})
    scores_dict.update({'PID type': pid_type})
    print(evaluation_result['request']['object_identifier'])
    print(result_string)
    return scores_dict


def push_status_to_googlesheets(googlesheet_url, row, status, start_time=None, end_time=None, duration=None, error_message=None):
    sheet_authorize = pygsheets.authorize(client_secret=config['GOOGLE']['credentials_filename'])
    fair_google_sheet = sheet_authorize.open_by_url(googlesheet_url).worksheet('title', 'EVAL')
    fair_google_sheet.update_value('K' + str(row), status)  # Evaluation status
    if error_message:
        fair_google_sheet.update_value('C' + str(row), error_message)  # Reuse cell to publish error message
        return False
    if start_time:
        fair_google_sheet.update_value('L' + str(row), start_time.strftime("%d-%b-%Y, %H:%M:%S"))  #
        fair_google_sheet.update_value('M' + str(row), '')  # empty the end time cell
    if end_time:
        fair_google_sheet.update_value('M' + str(row), end_time.strftime("%d-%b-%Y, %H:%M:%S"))
    if duration:
        fair_google_sheet.update_value('N' + str(row), str(duration))


def push_results_to_googlesheets(googlesheet_url, row, scores_dict):
    sheet_authorize = pygsheets.authorize(client_secret=config['GOOGLE']['credentials_filename'])
    fair_google_sheet = sheet_authorize.open_by_url(googlesheet_url).worksheet('title', 'EVAL')
    result_id = scores_dict['datasetID']
    print(fair_google_sheet.get_value('B' + str(row)).rstrip())
    print(result_id)
    if fair_google_sheet.get_value('B' + str(row)).rstrip() == result_id:
        # write values to sheet
        fair_google_sheet.update_value('D' + str(row), scores_dict['Result string'])
        fair_google_sheet.update_value('E' + str(row), scores_dict['F score'])
        fair_google_sheet.update_value('F' + str(row), scores_dict['A score'])
        fair_google_sheet.update_value('G' + str(row), scores_dict['I score'])
        fair_google_sheet.update_value('H' + str(row), scores_dict['R score'])
        fair_google_sheet.update_value('I' + str(row), scores_dict['Total score percent'])
        fair_google_sheet.update_value('J' + str(row), '(' + str(scores_dict["Total score"]) + ':' +
                                       str(scores_dict["Total points"]) + ')')
        fair_google_sheet.update_value('Z' + str(row), scores_dict['Success string'] + ', ' + scores_dict['doi'] +
                                       ', ' + scores_dict['PID type'])

        target_value = fair_google_sheet.get_value('B' + str(row)).rstrip()
        for element in scores_dict['Retrieving pages']:
            if target_value in element:
                fair_google_sheet.update_value('C' + str(row), scores_dict['Status code of FsF-F1-02D'])
                break
            else:
                fair_google_sheet.update_value('C' + str(row), 'N/A')
        return True
    else:
        print('Input in cell B%s does not match results of the processing %s, discarding results' % (row, result_id))
        return False


def process_fsf_evaluation(googlesheet_url, evaluator_url, candidates_for_processing, fair_google_sheet):
    for row, doi in candidates_for_processing.items():
        if fair_google_sheet.get_value('O1') == 'Run script':
            current_status = fair_google_sheet.get_value('K' + str(row))

            datacite_usage_request = fair_google_sheet.get_value('O2').capitalize()
            use_datacite = datacite_usage_request == 'True'

            print(use_datacite)
            if current_status in ['Analyzing', 'Error', 'Ready']:
                continue

            print(f'Processing {doi}')
            start_time = datetime.now()
            push_status_to_googlesheets(googlesheet_url, row, 'Analyzing', start_time=start_time)
            try:
                evaluation_result = push_to_fair_evaluator(evaluator_url, doi, use_datacite)
                end_time = datetime.now()
                print('Publishing results in Google Sheets.')
                scores_dict = calculate_score(evaluation_result)
                is_published = push_results_to_googlesheets(googlesheet_url, row, scores_dict)
                if is_published:
                    push_status_to_googlesheets(googlesheet_url, row, 'Ready', end_time=end_time,
                                                duration=end_time - start_time)
            except Exception as e:
                traceback.print_exc()
                push_status_to_googlesheets(googlesheet_url, row, 'Error', error_message=str(e))
                print('Failed.')
            print('Done.')
        else:
            print('Script stopped by User.')
            break


def script_start_check(googlesheet_url, evaluator_url):
    sheet_authorize = pygsheets.authorize(client_secret=config['GOOGLE']['credentials_filename'])
    fair_google_sheet = sheet_authorize.open_by_url(googlesheet_url).worksheet('title', 'EVAL')
    # check if script should be running
    if fair_google_sheet.get_value('O1') == 'Run script':
        candidates_list_for_processing = guid_collection(googlesheet_url)
        process_fsf_evaluation(googlesheet_url, evaluator_url, candidates_list_for_processing, fair_google_sheet)


def main():
    while True:
        try:
            googlesheet_url = config['GOOGLE']['googlesheet_url']
            evaluator_url = config['EVALUATOR']['evaluator_url']
            script_start_check(googlesheet_url, evaluator_url)
        except Exception as e:
            traceback.print_exc()
            print('Failed. Cause of the error: ' + str(e))
            time.sleep(60)
            continue
        else:
            time.sleep(60)


main()
