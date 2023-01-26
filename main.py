from datetime import datetime

from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import Session

from db_engines.sql_server_engine import engine_SQLServerTest_MainDB
from ddl_scripts.creating_tables import SignalMeta
import dql_scripts.simple_select as simple_select

simple_select.select_signalmeta_testobjs()  # Returns a _engine.Result obj with Row objs

simple_select.select_signalmeta_testobjs_scalar_result()  # Returns selection result as a Scalar obj

