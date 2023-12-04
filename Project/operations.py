import json

from sqlalchemy import insert, distinct, select, text
from sqlalchemy.orm import Session

from setup import BaseSpaceX

class SpaceXOperations(BaseSpaceX):

    def selectData(self, table, select_statement):
        query_result = []
        row_counter = 0
        with Session(self.engine) as session:
            for row in session.execute(select_statement):
                query_result.append(row)
                row_counter +=1 
            print(f'Query size: {row_counter}')
        return query_result

    def insertData(self, table, insert_rows):
        with Session(self.engine) as session:
            session.execute(
                insert(table),
                insert_rows,
            )
            session.commit()

    def dropData(self, table):
        table.drop(self.engine)

if __name__ == "__main__":
    space_api = SpaceXOperations()
    #Returning this object outside the class gives me the freedom to specify which table I want to operate on
    SLTable = space_api.createStarlinkTable()
    print(SLTable.c.keys())
    space_api.instantiateDB()
    print(space_api.gatherStarlinkJsonData()[0:5])
    starlink_data = space_api.gatherStarlinkJsonData()
    space_api.insertData(table=SLTable, insert_rows=starlink_data)
    s = select(SLTable).where(SLTable.c.satellite_id == '60106f21e900d60006e32cc7')
    print(space_api.selectData(table=SLTable, select_statement = s))
    s = select(distinct(SLTable.c.satellite_id)).limit(5)
    print(space_api.selectData(table=SLTable, select_statement = s))
    space_api.dropData(table=SLTable)



