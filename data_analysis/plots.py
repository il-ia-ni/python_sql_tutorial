""" This Script contains methods for creating Matplotlib plots from the results of data analysis """
from datetime import datetime

import pandas as pd
# Get default color style from pandas - can be changed to any other color list
from pandas.plotting._matplotlib.style import get_standard_colors
import matplotlib as mpl
import matplotlib.pyplot as plt
from loguru import logger

from data_analysis.dataframes import df_from_group
from data_analysis.defect_event_root_cause import join_defect_event_root_cause_filter_behaviour_id, BehaviourPattern
from db_engines.db_sources_data.sql_server_test_localhost import SQLServerTestDBs
from db_engines.sql_server_engine import url_SQLServerTestDBMS as sql_server_url
from db_engines.sqlite_engine import url_hostname as sqlite_url
from phillip.db_connection import create_session_from_url


# TODO: Create a script of caster data (also for caster filtering in data analysis scripts!)
# TODO: need a dict for caster 1_1
# dict_lim_2c_parent = {'superheat_C': {'min_value': 2700.0, 'max_value': 2900.0, 'color': 'b', 'unit': 'superheat [° F]'},
#                       'tundish_weight_C': {'min_value': 700.0, 'max_value': 2100.0, 'color': 'g', 'unit': 'tundish weight [klbs]'},
#                       'cast_speed_C': {'min_value': 20.0, 'max_value': 40.0, 'color': 'r', 'unit': 'cast speed [ipm]'},
#                        'mold_taper_C_N': {'min_value': 1.2, 'max_value': 2.2, 'color': 'c', 'unit': 'mold taper [inches]'},
#                        'argon_pressure_shroud_C': {'min_value': 85.0, 'max_value': 90.0, 'color': 'm', 'unit': 'argon pressure shroud [PSI]'},
#                        'stopper_rod_C1_C': {'min_value': 12.0, 'max_value': 28.0, 'color': 'y', 'unit': 'stopper rod [%]'},
#                        'mold_level_C_C1': {'min_value': 2.0, 'max_value': 10.0, 'color': 'k', 'unit': 'mold level [inches]'}}


def plot_multi(data: pd.DataFrame, title: str, defect: str, dict_lim=None, cols=None, spacing=.1, **kwargs):
    """ Creates Matplotlib figures based on config params and fills it with data
    :param data: a DataFrame of signals for analysis of a specified behaviour grouped by slab_id attribute
    :param title: a behaviour name to display in created plots
    :param defect: a detected defect name
    :param dict_lim: a dictionary of statistic and formatting params for signals of a caster
    :param cols: optional list of columns names for the DataFrame
    :param spacing: optional value for setting a distance between spines of the axes of plots
    :param kwargs: Splat-Operator for any other dictionary with key-values pairs to be used for plotting
    :return: a Matplotlib Axe-Object with the created plot
    """
    label_size = 14
    mpl.rcParams['xtick.labelsize'] = label_size
    if cols is None: cols = data.columns
    if len(cols) == 0: return  # TODO: Should this rather raise an Exception or fetch cols from a DataFrame?
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
        max_value = 1000  # TODO: Should this be calculated from averages of the DataFrame?
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
            max_value = 1000  # TODO: Should this be calculated from averages of the DataFrame?

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
    """ Groups each DataFrame from a dictionary of slab_ids and created plots for the analysed behaviour
    :param data: a dictionary of slab_id keys with values of DataFrames of signals of a specified behaviour
    :param defect: a detected defect name
    :param dict_lim: a dictionary of statistic and formatting params for signals of a caster
    :return: nothing is returned
    """
    for key, value in data.items():
        plt.figure(figsize=(10, 8))
        plot_multi(data=value, title=key, defect=defect, dict_lim=dict_lim)
        plt.show()


# For direct script execution without calling its methods in main.py:
if __name__ == "__main__":
    # More to dunder name variable __name__: https://www.pythontutorial.net/python-basics/python-__name__/

    app_odbc_driver = ""
    URL = sqlite_url
    session = create_session_from_url(url=URL, odbc_driver=app_odbc_driver)

    root_causes_strand_1_1_sliver = join_defect_event_root_cause_filter_behaviour_id(session=session,
                                                                                     defects=[
                                                                                         BehaviourPattern.CLOGGING],
                                                                                     strand_ids=['1_1'],
                                                                                     start=datetime.strptime(
                                                                                         '2023-01-16 00:00:00',
                                                                                         '%Y-%m-%d %H:%M:%S'),
                                                                                     end=datetime.strptime(
                                                                                         '2023-01-19 00:00:00',
                                                                                         '%Y-%m-%d %H:%M:%S'))
    groups = root_causes_strand_1_1_sliver.groupby(['slab_id'])
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html

    root_causes_strand_1_1_sliver_dict = {}
    for name, group in groups:
        group_df = df_from_group(group=group)
        root_causes_strand_1_1_sliver_dict[name] = group_df
    logger.info(root_causes_strand_1_1_sliver_dict)

    plot_multi_from_dict(root_causes_strand_1_1_sliver_dict, "Slivers")
