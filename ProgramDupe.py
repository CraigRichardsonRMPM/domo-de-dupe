import requests
from pydomo import Domo
from pydomo.datasets import DataSetRequest
from pydomo.datasets import Schema
from pydomo.datasets import Column
from pydomo.datasets import ColumnType
import json
import base64

client_id = 'YOUR CLIENT ID'
client_secret = 'YOUR CLIENT SECRET'
api_host = 'api.domo.com'
domo = Domo(client_id, client_secret, api_host)
dataset_id = 'YOUR DATASET ID'
pbs_media_manager_username = 'YOUR MEDIA MANAGER USERNAME'
pbs_media_manager_password = 'YOUR MEDIA MAANGER PASSWORD'
encoded_credentials = base64.b64encode(f"{pbs_media_manager_username}:{pbs_media_manager_password}".encode('utf-8')).decode('utf-8')
headers = {
    'Authorization': f'Basic {encoded_credentials}',
    'Content-Type': 'application/json'
}

def get_show_info_from_tp_media_id(tp_media_id):
    url = f'https://media.services.pbs.org/api/v1/assets/legacy/?tp_media_id={tp_media_id}'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if data.get('data') and data['data'].get('attributes'):
            attributes = data['data']['attributes']
            object_type = attributes.get('object_type', '')
            try:
                if 'parent_tree' in attributes:
                    parent_tree = attributes['parent_tree']
                    if 'show' in parent_tree:
                        show_id = parent_tree['show'].get('id')
                        show_slug = parent_tree['show']['attributes'].get('slug')
                    elif 'attributes' in parent_tree and 'season' in parent_tree['attributes']:
                        season = parent_tree['attributes']['season']
                        if 'show' in season['attributes']:
                            show = season['attributes']['show']
                            show_id = show.get('id')
                            show_slug = show['attributes'].get('slug')
                    else:
                        show_id = None
                        show_slug = None
                else:
                    if 'show' in attributes:
                        show_id = attributes['show'].get('id')
                        show_slug = attributes['show'].get('slug')
                    else:
                        show_id = None
                        show_slug = None
                return show_id, show_slug
            except (KeyError, TypeError, IndexError) as e:
                print(f"Error extracting show info: {e}")
                return None, None
        else:
            print("Response JSON does not contain expected 'data' or 'attributes' keys.")
            return None, None
    else:
        print(f"Failed to fetch show info due to API error. Status Code: {response.status_code}")
        return None, None

targeted_program_names = ["Professor T", "Before We Die", "The Life of Loi: Mediterranean Secrets"]

def structure_episodes_data(rows, target_programs):
    episodes = {}
    for row in rows:
        program_name, episode_name, tp_media_id, event_label, stream_count = row
        episode_data = {
            "tp_media_id": tp_media_id,
            "stream_count": stream_count,
            "event_label": event_label,
        }
        if program_name in target_programs:
            show_id, show_slug = get_show_info_from_tp_media_id(tp_media_id)
            if show_id and show_slug:
                episode_data["show_id"] = show_id
                episode_data["show_slug"] = show_slug
        if "full_length" in event_label:
            episodes[episode_name] = episode_data
    return episodes

sql_query_programs = "SELECT `Program`, SUM(`Total Events`) AS `totalViews` FROM `GeneralAudience_[krma]` WHERE `Date` >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND `Event Action` = 'MediaStart' AND `Program` != 'Livestream' GROUP BY `Program` ORDER BY `totalViews` DESC LIMIT 25"
data_structure = {
    "data": {
        "popular_programs": {},
        "trending_programs": {}
    }
}

def filter_full_length_episodes(episodes_data):
    filtered_episodes = {}
    for episode_name, episode_info in episodes_data.items():
        if "full_length" in episode_info.get("event_label", ""):
            filtered_episodes[episode_name] = episode_info
    return filtered_episodes

for program_name, program_data in data_structure["data"]["popular_programs"].items():
    filtered_episodes = filter_full_length_episodes(program_data)
    data_structure["data"]["popular_programs"][program_name] = filtered_episodes

for program_name, program_data in data_structure["data"]["trending_programs"].items():
    filtered_episodes = filter_full_length_episodes(program_data)
    data_structure["data"]["trending_programs"][program_name] = filtered_episodes

response = domo.datasets.query(dataset_id, sql_query_programs)
popular_programs_data = response.get('rows', [])
for program_info in popular_programs_data:
    program_name = program_info[0]
    sql_query_videos = "SELECT `Program`, `Video Name`, `TP Media ID`, `Event Label`, SUM(`Total Events`) AS `TotalViews` FROM `GeneralAudience_[krma]` WHERE `Program` = '"+program_name+"' AND `Date` >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) GROUP BY `Program`, `Video Name`, `TP Media ID`, `Event Label` ORDER BY `TotalViews` DESC"
    program_response = domo.datasets.query(dataset_id, sql_query_videos)
    data_structure["data"]["popular_programs"][program_name] = structure_episodes_data(program_response.get('rows', []), targeted_program_names)

sql_query_trending = "SELECT p.Program, (SUM(CASE WHEN p.Date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE() THEN p.`Total Events` ELSE 0 END) / NULLIF(SUM(CASE WHEN p.Date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN p.`Total Events` ELSE 0 END), 0)) AS GrowthRate FROM `GeneralAudience_[krma]` p INNER JOIN (SELECT `Program`, SUM(`Total Events`) AS TotalEvents30Days FROM `GeneralAudience_[krma]` WHERE `Date` BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE() AND `Event Action` = 'MediaStart' GROUP BY `Program` ORDER BY TotalEvents30Days DESC LIMIT 70) AS TopPrograms ON p.Program = TopPrograms.Program WHERE p.`Date` >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) AND p.`Event Action` = 'MediaStart' GROUP BY p.Program ORDER BY GrowthRate DESC LIMIT 30"
trending_programs_data = domo.datasets.query(dataset_id, sql_query_trending).get('rows', [])
for program_info in trending_programs_data:
    program_name = program_info[0]
    program_name_cleaned = program_name.replace("'", "")
    print(f"Querying for program: {program_name_cleaned}")
    sql_query_trending_videos = "SELECT `Program`, `Video Name`, `TP Media ID`, `Event Label`, SUM(`Total Events`) AS `TotalViews` FROM `GeneralAudience_[krma]` WHERE `Program` = '"+program_name_cleaned+"' AND `Date` >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) GROUP BY `Program`, `Video Name`, `TP Media ID`, `Event Label` ORDER BY `TotalViews` DESC"
    try:
        trending_program_response = domo.datasets.query(dataset_id, sql_query_trending_videos)
        if trending_program_response:
            data_structure["data"]["trending_programs"][program_name] = structure_episodes_data(trending_program_response.get('rows', []), targeted_program_names)
        else:
            print(f"No data returned for program: {program_name_cleaned}")
    except Exception as e:
        print(f"Failed to execute query for program {program_name_cleaned}: {e}")

def reorganize_and_rank_programs(data_structure, targeted_program_names):
    new_structure = {"popular_programs": {}, "trending_programs": {}}
    for category, programs in data_structure["data"].items():
        aggregated_data = {}
        for program_name, episodes in programs.items():
            for episode_name, episode_info in episodes.items():
                if program_name in targeted_program_names and 'show_id' in episode_info:
                    key = episode_info['show_slug'].replace('-', ' ').title()
                else:
                    key = program_name
                if key not in aggregated_data:
                    aggregated_data[key] = {'total_views': 0, 'episodes': {}, 'aggregated_stream_count': 0}
                aggregated_data[key]['episodes'][episode_name] = episode_info
               
                aggregated_data[key]['aggregated_stream_count'] += int(episode_info.get('stream_count', 0))
        
       
        sorted_programs = sorted(aggregated_data.items(), key=lambda x: x[1]['aggregated_stream_count'], reverse=True)
        
       
        for program in sorted_programs:
            program_name, program_data = program
            new_structure[category][program_name] = {
                'episodes': program_data['episodes'],
                'aggregated_stream_count': program_data['aggregated_stream_count']
            }

    
    return {"data": new_structure}


data_structure = reorganize_and_rank_programs(data_structure, targeted_program_names)


with open('program_data.json', 'w', encoding='utf-8') as f:
    json.dump(data_structure, f, ensure_ascii=False, indent=4)

print("Ranked program data has been written to program_data.json")
