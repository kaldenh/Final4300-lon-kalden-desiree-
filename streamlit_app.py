import streamlit as st
import pandas as pd
from st_files_connection import FilesConnection

# Set page configuration
st.set_page_config(
    page_title="CSV File Uploader",
    page_icon="📊",
    layout="wide"
)

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
    

    except Exception as e:
        st.error(f"Error reading the CSV file: {e}")

    if st.button("Write to S3"):
        conn = st.connection('s3', type=FilesConnection)
        conn.write(df, 'ds4300-final-lon-kalden-desiree/Example_Pokemon_Team_Bad.csv', output_format="csv", index=False)
        st.success("File written to S3 successfully!")
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

