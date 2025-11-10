import streamlit as st
import pandas as pd
import os
from io import BytesIO

# Expected columns
EXPECTED_COLUMNS = {"Fruits", "Owner", "Age"}

# App title
st.title("🍉 CSV Uploader with Schema Check & Age Sum Processor")

# File uploader
uploaded_file = st.file_uploader("Upload a CSV file", type=None)

if uploaded_file is not None:
    filename = uploaded_file.name
    file_extension = os.path.splitext(filename)[1].lower()

    # Validate extension
    if file_extension == ".csv":
        try:
            # Read CSV
            df = pd.read_csv(uploaded_file)
            uploaded_columns = set(df.columns)

            # Validate schema
            missing_columns = EXPECTED_COLUMNS - uploaded_columns
            extra_columns = uploaded_columns - EXPECTED_COLUMNS

            if not missing_columns:
                st.success(f"✅ '{filename}' uploaded successfully with correct schema!")
                st.write("**Columns found:**", list(df.columns))
                st.dataframe(df.head())

                if extra_columns:
                    st.warning(f"⚠️ Extra columns found: {list(extra_columns)}")

                # --- PROCESSING SECTION ---
                try:
                    # Ensure Age is numeric
                    df["Age"] = pd.to_numeric(df["Age"], errors="coerce")

                    # Compute sum of Age
                    age_sum = df["Age"].sum()

                    st.success(f"🧮 The sum of all values in 'Age' column is: {age_sum}")

                    # Create a result DataFrame
                    result_df = pd.DataFrame({"Total_Age_Sum": [age_sum]})

                    # Save result to in-memory buffer
                    buffer = BytesIO()
                    result_df.to_csv(buffer, index=False)
                    buffer.seek(0)

                    # Download button
                    st.download_button(
                        label="⬇️ Download Result File (age_sum.csv)",
                        data=buffer,
                        file_name="age_sum.csv",
                        mime="text/csv",
                    )
                except Exception as e:
                    st.error(f"⚠️ Error processing 'Age' column: {e}")

            else:
                st.error("❌ Invalid schema detected!")
                st.write("**Missing columns:**", list(missing_columns))
                st.write("**Expected columns:**", list(EXPECTED_COLUMNS))
                st.write("**Found columns:**", list(df.columns))

        except Exception as e:
            st.error(f"⚠️ Error reading CSV: {e}")

    else:
        st.error("❌ Invalid file type! Please upload a file with '.csv' extension only.")
else:
    st.info("Please upload a CSV file with columns: Fruits, Owner, Age")

# streamlit run subirArchivo.py