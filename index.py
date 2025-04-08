import requests
import json
import pandas as pd
import os
import time
import logging
import gzip
import base64

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_html_file_with_url(file_path, api_key):
    """
    Instead of sending the entire HTML content, extract the URL from the file
    and send that to the API.
    
    Args:
        file_path (str): Path to the HTML file
        api_key (str): API key for the XBRL-to-JSON converter
        
    Returns:
        dict: JSON data containing the processed financial statements, or None if processing failed
    """
    try:
        # Get file name for logging
        file_name = os.path.basename(file_path)
        
        # Read the first few lines to extract the URL if it exists
        with open(file_path, 'r', encoding='utf-8') as file:
            first_lines = ''.join([file.readline() for _ in range(20)])
            
        # This is a fallback approach - in practice, you should know the URL
        # of where you downloaded the file from
        logger.info(f"Using URL method for {file_name}")
        
        # Use SEC API with the URL parameter instead of HTML content
        # You would need the original SEC URL for the filing
        # For demonstration, we're using a placeholder URL
        # Replace with actual URL pattern for your files
        cik = "320193"  # Apple's CIK
        year = file_name.split('_')[2].split('.')[0] if len(file_name.split('_')) > 2 else "2021"
        
        # Construct a URL pattern - this is an example and should be adjusted based on actual file naming
        sec_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/000032019{year}000056/aapl-{year}0327.htm"
        
        # XBRL-to-JSON converter API endpoint with URL parameter
        xbrl_converter_api_endpoint = "https://api.sec-api.io/xbrl-to-json"
        final_url = f"{xbrl_converter_api_endpoint}?htm-url={sec_url}&token={api_key}"
        
        logger.info(f"Requesting conversion for URL: {sec_url}")
        response = requests.get(final_url)
        
        # Check if request was successful
        if response.status_code == 200:
            logger.info(f"Successfully processed {file_name}")
            return json.loads(response.text)
        else:
            logger.error(f"Error processing {file_name} with URL method: HTTP {response.status_code}, {response.text[:200]}")
            return None
    
    except Exception as e:
        logger.error(f"Exception while processing {file_path}: {str(e)}")
        return None

def process_html_file_chunked(file_path, api_key):
    """
    Process an HTML file by splitting it into chunks
    or compressing it before sending to the API.
    
    Args:
        file_path (str): Path to the HTML file
        api_key (str): API key for the XBRL-to-JSON converter
        
    Returns:
        dict: JSON data containing the processed financial statements, or None if processing failed
    """
    try:
        # Read the HTML file content
        with open(file_path, 'rb') as file:
            html_content = file.read()
        
        # Compress the content with gzip
        compressed_content = gzip.compress(html_content)
        
        # Encode as base64
        encoded_content = base64.b64encode(compressed_content).decode('ascii')
        
        logger.info(f"Original size: {len(html_content)} bytes, Compressed: {len(compressed_content)} bytes")
        
        # XBRL-to-JSON converter API endpoint
        xbrl_converter_api_endpoint = "https://api.sec-api.io/xbrl-to-json"
        
        # Make request to the API with compressed content
        headers = {
            'Content-Type': 'application/json',
            'Content-Encoding': 'gzip',
            'Accept-Encoding': 'gzip'
        }
        
        payload = {
            'html_compressed': encoded_content,  # Sending compressed content
            'token': api_key
        }
        
        logger.info(f"Sending compressed request for file: {os.path.basename(file_path)}")
        response = requests.post(xbrl_converter_api_endpoint, headers=headers, json=payload)
        
        # Check if request was successful
        if response.status_code == 200:
            logger.info(f"Successfully processed {os.path.basename(file_path)}")
            return json.loads(response.text)
        else:
            logger.error(f"Error processing {file_path} with compression: HTTP {response.status_code}, {response.text[:200]}")
            return None
    
    except Exception as e:
        logger.error(f"Exception while processing {file_path}: {str(e)}")
        return None

def extract_accession_number_from_filename(filename):
    """
    Extract accession number from filename if it follows a pattern
    This is a placeholder - adapt to your actual file naming convention
    """
    parts = filename.split('_')
    if len(parts) >= 3:
        return parts[2].split('.')[0]
    return None

def extract_income_statement(json_data, source_file):
    """
    Extract income statement from JSON data
    
    Args:
        json_data (dict): JSON data from XBRL-to-JSON converter
        source_file (str): Name of the source file
        
    Returns:
        DataFrame: Pandas DataFrame containing the income statement data
    """
    if 'StatementsOfIncome' not in json_data:
        logger.warning(f"No income statement found in {source_file}")
        return None
    
    income_statement = pd.DataFrame()
    
    # Extract each item in the income statement
    for item, values in json_data['StatementsOfIncome'].items():
        for value in values:
            period = value['period']
            if period not in income_statement.columns:
                income_statement[period] = None
            income_statement.loc[item, period] = value['value']
    
    # Add source file information
    income_statement['source_file'] = source_file
    
    return income_statement

def extract_balance_sheet(json_data, source_file):
    """Extract balance sheet from JSON data"""
    if 'BalanceSheets' not in json_data:
        logger.warning(f"No balance sheet found in {source_file}")
        return None
    
    balance_sheet = pd.DataFrame()
    
    for item, values in json_data['BalanceSheets'].items():
        for value in values:
            date = value['date']
            if date not in balance_sheet.columns:
                balance_sheet[date] = None
            balance_sheet.loc[item, date] = value['value']
    
    balance_sheet['source_file'] = source_file
    return balance_sheet

def extract_cash_flow(json_data, source_file):
    """Extract cash flow statement from JSON data"""
    if 'StatementsOfCashFlows' not in json_data:
        logger.warning(f"No cash flow statement found in {source_file}")
        return None
    
    cash_flow = pd.DataFrame()
    
    for item, values in json_data['StatementsOfCashFlows'].items():
        for value in values:
            period = value['period']
            if period not in cash_flow.columns:
                cash_flow[period] = None
            cash_flow.loc[item, period] = value['value']
    
    cash_flow['source_file'] = source_file
    return cash_flow

def main():
    # Your API key - replace with your actual key
    api_key = "44ba705581dee21a56a223d5418b0d944702a85ac447047ed3a4b1f6f2ace0db"
    
    # List of HTML files to process
    html_files = [
        "aapl_10k_1.html",
        "aapl_10k_2.html",
        "aapl_10k_3.html",
        "aapl_10k_4.html",
        "aapl_10k_5.html"
    ]
    
    # Process each file individually
    results = {}
    
    for file_path in html_files:
        logger.info(f"Processing file: {file_path}")
        
        # Method 1: Try using the URL parameter instead of sending HTML content
        result = process_html_file_with_url(file_path, api_key)
        
        # If URL method fails, try the alternative methods
        if not result:
            logger.info(f"URL method failed for {file_path}, trying alternative methods")
            
            # Method 2: Use direct SEC Edgar access
            # For this approach, we need to know the actual filing details
            # This would be a more specific implementation based on your file naming convention
            accession_number = extract_accession_number_from_filename(file_path)
            if accession_number:
                cik = "320193"  # Apple's CIK
                filing_year = "20" + accession_number[:2]  # Extract year from accession
                
                # Construct a URL for SEC Edgar API
                edgar_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{filing_year}{accession_number}/xbrl.zip"
                
                # You would implement the XBRL processing here
                # This is a placeholder for custom XBRL processing logic
                logger.info(f"Would process XBRL from {edgar_url}")
        
        if result:
            file_name = os.path.basename(file_path)
            results[file_name] = result
        
        # Sleep to avoid overwhelming the API
        time.sleep(2)
    
    # Extract financial statements from all processed files
    all_income_statements = []
    all_balance_sheets = []
    all_cash_flows = []
    
    for file_name, data in results.items():
        # Extract income statement
        income_statement = extract_income_statement(data, file_name)
        if income_statement is not None:
            all_income_statements.append(income_statement)
        
        # Extract balance sheet
        balance_sheet = extract_balance_sheet(data, file_name)
        if balance_sheet is not None:
            all_balance_sheets.append(balance_sheet)
        
        # Extract cash flow statement
        cash_flow = extract_cash_flow(data, file_name)
        if cash_flow is not None:
            all_cash_flows.append(cash_flow)
    
    # Combine all financial statements
    if all_income_statements:
        combined_income_statement = pd.concat(all_income_statements)
        combined_income_statement.to_csv('combined_income_statement.csv')
        logger.info("Combined income statement saved to 'combined_income_statement.csv'")
    
    if all_balance_sheets:
        combined_balance_sheet = pd.concat(all_balance_sheets)
        combined_balance_sheet.to_csv('combined_balance_sheet.csv')
        logger.info("Combined balance sheet saved to 'combined_balance_sheet.csv'")
    
    if all_cash_flows:
        combined_cash_flow = pd.concat(all_cash_flows)
        combined_cash_flow.to_csv('combined_cash_flow.csv')
        logger.info("Combined cash flow statement saved to 'combined_cash_flow.csv'")
    
    # Print summary
    logger.info(f"Total files processed: {len(results)}")
    logger.info(f"Income statements extracted: {len(all_income_statements)}")
    logger.info(f"Balance sheets extracted: {len(all_balance_sheets)}")
    logger.info(f"Cash flow statements extracted: {len(all_cash_flows)}")

if __name__ == "__main__":
    main()