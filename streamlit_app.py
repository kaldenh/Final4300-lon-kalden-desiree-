import streamlit as st
import pandas as pd
import io
from st_files_connection import FilesConnection

# Set page configuration
st.set_page_config(
    page_title="CSV File Uploader",
    page_icon="📊",
    layout="wide"
)

conn = st.connection('s3', type=FilesConnection)
df = conn.read("testbucket-jrieke/myfile.csv", input_format="csv")
# Print results.
for row in df.itertuples():
    st.write(f"{row}")


# App title and description
st.title("CSV File Uploader")
st.markdown("Upload a CSV file to view and analyze its contents")

# File uploader widget
uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

# Check if a file was uploaded
if uploaded_file is not None:
    # Display success message
    st.success("File successfully uploaded!")
    
    # Display file details
    file_details = {
        "Filename": uploaded_file.name,
        "File size": f"{uploaded_file.size / 1024:.2f} KB"
    }
    
    st.write("### File Details")
    for key, value in file_details.items():
        st.write(f"**{key}:** {value}")
    
    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(uploaded_file)
        
        # Display the number of rows and columns
        st.write(f"**Rows:** {df.shape[0]} | **Columns:** {df.shape[1]}")
        
        # Display DataFrame information
        st.write("### Data Preview")
        st.dataframe(df.head(10), use_container_width=True)
        
        # Basic data info
        st.write("### Data Information")
        
        # Show column names and data types
        col_info = pd.DataFrame({
            'Column Name': df.columns,
            'Data Type': df.dtypes.astype(str),
            'Non-Null Count': df.count().values,
            'Null Count': df.isnull().sum().values
        })
        st.dataframe(col_info, use_container_width=True)
        
        # # Add some data analysis options
        # st.write("### Data Analysis")
        
        # # Allow user to select columns for analysis
        # if len(df.columns) > 0:
        #     analysis_options = st.multiselect(
        #         "Select columns to analyze:",
        #         df.columns
        #     )
            
        #     if analysis_options:
        #         # Create tabs for different analyses
        #         tab1, tab2, tab3 = st.tabs(["Summary Statistics", "Value Counts", "Data Visualization"])
                
        #         with tab1:
        #             st.write("#### Summary Statistics")
                    
        #             # Get only numeric columns from selected columns
        #             numeric_cols = df[analysis_options].select_dtypes(include=['number']).columns
                    
        #             if not numeric_cols.empty:
        #                 st.dataframe(df[numeric_cols].describe(), use_container_width=True)
        #             else:
        #                 st.info("No numeric columns selected for summary statistics.")
                
        #         with tab2:
        #             st.write("#### Value Counts")
                    
        #             # Let user select a column for value counts
        #             if analysis_options:
        #                 column = st.selectbox("Select a column:", analysis_options)
                        
        #                 # Show value counts for the selected column
        #                 value_counts = df[column].value_counts().reset_index()
        #                 value_counts.columns = [column, 'Count']
                        
        #                 st.dataframe(value_counts, use_container_width=True)
                
        #         with tab3:
        #             st.write("#### Data Visualization")
                    
        #             # Get numeric columns for visualization
        #             numeric_cols = df[analysis_options].select_dtypes(include=['number']).columns
                    
        #             if not numeric_cols.empty:
        #                 # Let user select chart type
        #                 chart_type = st.selectbox(
        #                     "Select chart type:",
        #                     ["Bar Chart", "Line Chart", "Histogram"]
        #                 )
                        
        #                 # Let user select column to visualize
        #                 vis_column = st.selectbox("Select column to visualize:", numeric_cols)
                        
        #                 if chart_type == "Bar Chart":
        #                     st.bar_chart(df[vis_column])
        #                 elif chart_type == "Line Chart":
        #                     st.line_chart(df[vis_column])
        #                 elif chart_type == "Histogram":
        #                     # Create histogram using Streamlit
        #                     fig, ax = plt.subplots()
        #                     ax.hist(df[vis_column].dropna(), bins=20)
        #                     ax.set_xlabel(vis_column)
        #                     ax.set_ylabel('Frequency')
        #                     ax.set_title(f'Histogram of {vis_column}')
        #                     st.pyplot(fig)
        #             else:
        #                 st.info("No numeric columns selected for visualization.")
        
        # # Download options
        # st.write("### Download Processed Data")
        
        # # Convert DataFrame to CSV
        # csv = df.to_csv(index=False).encode('utf-8')
        
        # # Download button
        # st.download_button(
        #     label="Download CSV",
        #     data=csv,
        #     file_name=f"processed_{uploaded_file.name}",
        #     mime="text/csv",
        # )
        
    except Exception as e:
        st.error(f"Error reading the CSV file: {e}")
else:
    # Display instructions when no file is uploaded
    st.info("👆 Please upload a CSV file to get started.")
    
    # Example instructions
    st.markdown("""
    ### Instructions:
    1. Click the 'Browse files' button above
    2. Select a CSV file from your computer
    3. The file will be automatically uploaded and analyzed
    4. View the data preview and analysis results
    """)
    
    # Show a sample of what users can expect
    st.write("### Example Preview:")
    sample_data = pd.DataFrame({
        'Name': ['John', 'Lisa', 'Mike', 'Sarah'],
        'Age': [28, 34, 42, 31],
        'City': ['New York', 'Boston', 'Chicago', 'Seattle'],
        'Salary': [75000, 85000, 92000, 79000]
    })
    st.dataframe(sample_data, use_container_width=True)

# Add a footer
st.markdown("---")
st.markdown("CSV Uploader App | Created with Streamlit")