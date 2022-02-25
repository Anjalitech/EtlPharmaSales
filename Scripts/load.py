from DataBase.base import session
from DataBase.tables import PprRawAll, PprCleanAll
from sqlalchemy import cast, Integer, Date
from sqlalchemy.dialects.postgresql import insert


def insert_transactions():       
    clean_transaction_ids = session.query(PprCleanAll.transaction_id)

    transactions_to_insert = session.query(
        cast(PprRawAll.date_of_sale, Date),
        PprRawAll.address,
        PprRawAll.postal_code,
        PprRawAll.county,
        cast(PprRawAll.price, Integer),
        PprRawAll.description,
    ).filter(~PprRawAll.transaction_id.in_(clean_transaction_ids))
	
   
    print("Transactions to insert:", transactions_to_insert.count())   
    
    stm = insert(PprCleanAll).from_select(
        ["date_of_sale", "address", "postal_code", "county", "price", "description"],
        transactions_to_insert,
    )   
    session.execute(stm)
    session.commit()


def delete_transactions():   
    raw_transaction_ids = session.query(PprRawAll.transaction_id)    
    transactions_to_delete = session.query(PprCleanAll).filter(
        ~PprCleanAll.transaction_id.in_(raw_transaction_ids)
    )   
    
    print("Transactions to delete:", transactions_to_delete.count())   
    transactions_to_delete.delete(synchronize_session=False)
    session.commit()

def main():
    print("[Load] Start")
    print("[Load] Inserting new rows")
    insert_transactions()
    print("[Load] Deleting rows not available in the new transformed data")
    delete_transactions()
    print("[Load] End")