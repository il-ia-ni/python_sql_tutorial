import dql_scripts.simple_select as simple_select
import dql_scripts.select_joins as joined_selects

#simple_select.select_core_signalmeta_all()  # Selection from the table using Core API
#simple_select.select_orm_signalmeta_all()  # Selection from the table using ORM API

# simple_select.select_core_signalmeta_3cols()  # selects 3 cols from a Core API Table obj

#simple_select.select_orm_signalmeta_testobjs()  # Returns a _engine.Result obj with Row objs
#simple_select.select_orm_signalmeta_testobjs_scalar_result()  # Returns selection result as a Scalar obj

joined_selects.get_select_join_orm_result(joined_selects.select_join_orm_stmt1)  # Join with select.join_from()
joined_selects.get_select_join_orm_result(joined_selects.select_join_orm_stmt2)  # Join with select.join()
joined_selects.get_select_join_orm_result(joined_selects.select_join_orm_stmt3)  # Join with join() and explicit ON

