import dql_scripts.simple_select as simple_select

#simple_select.select_core_signalmeta_all()  # Selection from the table using Core API
#simple_select.select_orm_signalmeta_all()  # Selection from the table using ORM API

simple_select.select_core_signalmeta_3cols()  # selects 3 cols from a Core API Table obj

#simple_select.select_orm_signalmeta_testobjs()  # Returns a _engine.Result obj with Row objs
#simple_select.select_orm_signalmeta_testobjs_scalar_result()  # Returns selection result as a Scalar obj

