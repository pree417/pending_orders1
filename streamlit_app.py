# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col, when_matched
import pandas as pd
# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders. :cup_with_straw:")
st.write("Orders need to be filled")

# Establish connection to Snowflake
cnx = st.connection("snowflake")
session = cnx.session()

# Fetch pending orders (where ORDER_FILLED == 0)
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == 0).collect()

if my_dataframe:
    # Convert Snowflake Row object to Pandas DataFrame for editing
    pd_df = my_dataframe.to_pandas()
    editable_df = st.data_editor(pd_df)

    submitted = st.button('Submit')
    if submitted:
        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)
        
        try:
            # Perform the merge operation to update ORDER_FILLED status
            og_dataset.merge(
                edited_dataset,
                (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID']),
                [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
            ).collect()  # Collect the result to execute the operation
            
            st.success('Order updated successfully!', icon='👍')
        except Exception as e:
            st.write(f"Error occurred: {e}")
else:
    st.success('There are no pending orders right now', icon='👍')
