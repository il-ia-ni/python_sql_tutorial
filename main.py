import json
from datetime import datetime

import pandas as pd
from pandas.plotting._matplotlib.style import get_standard_colors
# Get default color style from pandas - can be changed to any other color list
import matplotlib as mpl
import matplotlib.pyplot as plt

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

        #print(signal_data)
        #print(json.loads(signal_data))
        signal_data_json = json.loads(json.loads(signal_data))  # TODO: why is there a doubled parsing of json to obj?
        print(signal_data_json)
        signal_data_df = pd.DataFrame(signal_data_json['data'], columns=['time', signal_data_json['signal_id']])
        signal_data_df['time'] = pd.to_datetime(signal_data_df['time'])
        signal_data_df = signal_data_df.set_index('time')
        df_list.append(signal_data_df)
    df_concate = pd.concat(df_list, axis=1)
    return df_concate


root_causes_strand_1_1_sliver = join_defect_event_root_cause_filter_behaviour_id(session=global_session,
                                                        defects=[BehaviourPattern.CLOGGING],
                                                        strand_ids=['1_1'],
                                                        start=datetime.strptime('2023-01-16 00:00:00',
                                                                                '%Y-%m-%d %H:%M:%S'),
                                                        end=datetime.strptime('2023-01-19 00:00:00',
                                                                              '%Y-%m-%d %H:%M:%S'))
groups = root_causes_strand_1_1_sliver.groupby(['slab_id'])
# https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html

root_causes_strand_1_1_sliver_dict = {}
for name, group in groups:
    group_df = df_from_group(group=group)
    root_causes_strand_1_1_sliver_dict[name] = group_df
logger.info(root_causes_strand_1_1_sliver_dict)


def plot_multi(data: pd.DataFrame, title: str, defect: str, dict_lim=None, cols=None, spacing=.1, **kwargs):
    label_size = 14
    mpl.rcParams['xtick.labelsize'] = label_size
    if cols is None: cols = data.columns
    if len(cols) == 0: return
    colors = get_standard_colors(num_colors=len(cols))

    # First axis
    if dict_lim is not None:
        ax = data.loc[:, cols[0]].plot(label=cols[0], color=dict_lim[cols[0]]['color'], **kwargs)
        # ax.set_ylim(30, 65)
        ax.set_ylabel(ylabel=dict_lim[cols[0]]['unit'], size=14)
        ax.yaxis.label.set_color(dict_lim[cols[0]]['color'])
        min_value = dict_lim[cols[0]]['min_value']
        max_value = dict_lim[cols[0]]['max_value']
        ax.set_ylim(min_value, max_value)
    else:
        ax = data.loc[:, cols[0]].plot(label=cols[0], color='blue', **kwargs)
        min_value = 0
        max_value = 1000
        ax.set_ylim(min_value, max_value)
    ax.set_xlabel(xlabel='time [s]', size=12)
    ax.tick_params(axis='both', which='major', labelsize=12)
    lines, labels = ax.get_legend_handles_labels()

    for n in range(1, len(cols)):
        # Multiple y-axes
        ax_new = ax.twinx()
        ax_new.spines['right'].set_position(('axes', 1 + spacing * (n - 1)))
        if dict_lim is not None:
            data.loc[:, cols[n]].plot(ax=ax_new, label=cols[n], color=dict_lim[cols[n]]['color'], **kwargs)
            ax_new.set_ylabel(ylabel=dict_lim[cols[n]]['unit'], size=14)
            ax_new.yaxis.label.set_color(dict_lim[cols[n]]['color'])
            min_value = dict_lim[cols[n]]['min_value']
            max_value = dict_lim[cols[n]]['max_value']
        else:
            data.loc[:, cols[n]].plot(ax=ax_new, label=cols[n], color='red', **kwargs)
            ax_new.set_ylabel(ylabel='test1', size=14)
            ax_new.yaxis.label.set_color('red')
            min_value = 0
            max_value = 1000

        ax_new.set_ylim(min_value, max_value)
        ax_new.tick_params(axis='both', which='major', labelsize=12)

        # Proper legend position
        line, label = ax_new.get_legend_handles_labels()
        lines += line
        labels += label

    ax.legend(lines, labels, loc='best', fontsize=12)
    plt.title(label="Defect: {defect};  Slab: {slab}".format(defect=defect, slab=title),
              fontsize=12)
    plt.tick_params(axis='x', which='major', labelsize=12)
    plt.grid()

    return ax


def plot_multi_from_dict(data: dict, defect: str, dict_lim=None):
    for key, value in data.items():
        figure = plt.figure(figsize=(10, 8))
        ax = plot_multi(data=value, title=key, defect=defect, dict_lim=dict_lim)
        plt.show()

# dict_lim_2c_parent = {'superheat_C': {'min_value': 2700.0, 'max_value': 2900.0, 'color': 'b', 'unit': 'superheat [° F]'},
#                       'tundish_weight_C': {'min_value': 700.0, 'max_value': 2100.0, 'color': 'g', 'unit': 'tundish weight [klbs]'},
#                       'cast_speed_C': {'min_value': 20.0, 'max_value': 40.0, 'color': 'r', 'unit': 'cast speed [ipm]'},
#                        'mold_taper_C_N': {'min_value': 1.2, 'max_value': 2.2, 'color': 'c', 'unit': 'mold taper [inches]'},
#                        'argon_pressure_shroud_C': {'min_value': 85.0, 'max_value': 90.0, 'color': 'm', 'unit': 'argon pressure shroud [PSI]'},
#                        'stopper_rod_C1_C': {'min_value': 12.0, 'max_value': 28.0, 'color': 'y', 'unit': 'stopper rod [%]'},
#                        'mold_level_C_C1': {'min_value': 2.0, 'max_value': 10.0, 'color': 'k', 'unit': 'mold level [inches]'}}

plot_multi_from_dict(root_causes_strand_1_1_sliver_dict, "Slivers")

