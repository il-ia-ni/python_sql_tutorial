import json
from datetime import datetime

import pandas as pd

from data_analysis.defect_event_root_cause import join_defect_event_root_cause_filter_behaviour_id, BehaviourPattern
from phillip.db_connection import get_session
from db_engines.sql_server_engine import engine_sqlservertest_main as sqlserver_engine

from loguru import logger
from loguru_logging.debug_formatter import debug_format

import dql_scripts
import dql_scripts.simple_select as smpl_sel
import dql_scripts.select_joins as jnt_sel

debug_format()  # adds a custom format for debug-level of loguru (saving local .log-files)
global_session = get_session(sqlserver_engine)

# TODO: Move following lines to corresponding __name__="__main__" scripts!
# smpl_sel.select_core_signalmeta_all(global_engine)  # Selection from the table using Core API
# smpl_sel.select_orm_signalmeta_all(global_session)  # Selection from the table using ORM API

# smpl_sel.select_core_signalmeta_3cols(global_engine)  # selects 3 cols from a Core API Table obj

# smpl_sel.select_orm_signalmeta_testobjs(global_session)  # Returns a _engine.Result obj with Row objs
# smpl_sel.select_orm_signalmeta_testobjs_scalar_result(global_session)  # Returns selection result as a Scalar obj

def df_from_group(group: pd.DataFrame):
    df_list = []
    for index, row in group.iterrows():
        signal_data = row['signal_data']
        # https://docs.sqlalchemy.org/en/14/dialects/mssql.html#sqlalchemy.dialects.mssql.JSON
        # https://arctype.com/blog/json-database-when-use/
        print(signal_data)
        signal_data_json = json.loads(json.loads(signal_data))
        print(signal_data_json)
        signal_data_df = pd.DataFrame(signal_data_json['data'], columns=['time', signal_data_json['signal_id']])
        signal_data_df['time'] = pd.to_datetime(signal_data_df['time'])
        signal_data_df = signal_data_df.set_index('time')
        df_list.append(signal_data_df)
    df_concate = pd.concat(df_list, axis=1)
    return df_concate


data = join_defect_event_root_cause_filter_behaviour_id(session=global_session,
                                                        defects=[BehaviourPattern.CLOGGING],
                                                        strand_ids=['1_1'],
                                                        start=datetime.strptime('2023-01-16 00:00:00',
                                                                                '%Y-%m-%d %H:%M:%S'),
                                                        end=datetime.strptime('2023-01-19 00:00:00',
                                                                              '%Y-%m-%d %H:%M:%S'))
groups = data.groupby(['slab_id'])
# https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html

group_dict = {}
for name, group in groups:
    group_df = df_from_group(group=group)
    group_dict[name] = group_df
logger.info(group_dict)

# df = pd.DataFrame(
#     [
#         ("bird", "Falconiformes", 389.0),
#         ("bird", "Psittaciformes", 24.0),
#         ("mammal", "Carnivora", 80.2),
#         ("mammal", "Primates", None),
#         ("mammal", "Carnivora", 58),
#     ],
#     index=["falcon", "parrot", "lion", "monkey", "leopard"],
#     columns=("class", "order", "max_speed"),
# )
#
# grouped1 = df.groupby("class")
# logger.info(grouped1.get_group("mammal"))
#
# grouped2 = df.groupby("order", axis="columns")
# for name, group in grouped2:
#     logger.info(f"""{name} has a group: {group}""")
#
# grouped3 = df.groupby(["class", "order"])
# for name, group in grouped3:
#     logger.info(f"""{name} has a group: {group}""")