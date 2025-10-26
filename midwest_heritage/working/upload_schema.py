from midwest_heritage.utils.sql_manager import manager
from midwest_heritage.utils.utils import go_up_dirs
import os
if __name__ == "__main__":
    # Initialize database manager and connect to the database (default behavior)
    db = manager()

    # Upload/execute the given schema
    price_list_schema_path = os.path.join(go_up_dirs(__file__, 2), 'inputs', 'price_list_schema.sql')
    db.upload_schema(price_list_schema_path)

    # Close the database connection
    db.close()