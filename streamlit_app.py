# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders. :cup_with_straw:")
st.write("Orders need to be filled")

# Establish connection to Snowflake
cnx = st.connection("snowflake")
session = cnx.session()

# Fetch pending orders (where ORDER_FILLED == 0) as a Snowpark DataFrame
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == 0)

# Convert Snowpark DataFrame to Pandas DataFrame directly
pd_df = my_dataframe.to_pandas()

if not pd_df.empty:
    # Show editable data
    editable_df = st.data_editor(pd_df)

    submitted = st.button('Submit')
    if submitted:
        # Re-create the Snowpark DataFrame directly from Pandas DataFrame
        edited_dataset = session.create_dataframe(editable_df)

        try:
            # Perform the merge operation to update ORDER_FILLED status in Snowflake
            my_dataframe.merge(
                edited_dataset,
                on="ORDER_UID",  # Ensure the correct column to merge
                when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})
            ).collect()  # Execute the operation
            
            st.success('Order updated successfully!', icon='👍')
        except Exception as e:
            st.write(f"Error occurred: {e}")
else:
    st.success('There are no pending orders right now', icon='👍')
