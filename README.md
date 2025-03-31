# Shopping Scraper Data Import

A Streamlit application for importing shopping scraper Excel data into a Supabase database.

## Features

- Import data from Excel files containing shopping scraper data
- Support for different scrape types (Products Scrape and Shopping Tab Scrape)
- Link scrape data to existing campaigns and clients
- Simple and user-friendly interface

## Setup

### Prerequisites

- Python 3.8 or higher
- Supabase account with existing campaigns and clients tables

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourname/shopping-scraper-import.git
   cd shopping-scraper-import
   ```

2. Create and activate a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   - Copy `.env.template` to `.env`
   - Edit `.env` and add your Supabase credentials

5. Set up the database schema:
   - Run the SQL commands in `database/schema.sql` in your Supabase SQL editor

### Running the Application

```
streamlit run main.py
```

The application will be available at http://localhost:8501

### Exposing with ngrok (Optional)

If you need to make the application accessible from outside your network:

1. Install ngrok
2. Run `ngrok http 8501`
3. Use the URL provided by ngrok

## Database Structure

The application uses the following database tables:

- `scrape_types`: Stores the different types of scrapes (Products, Shopping Tab)
- `scrape_data`: Stores all the scrape information linked to campaigns

## Usage

1. Select a campaign (which will be linked to a client)
2. Choose the type of scrape (Products or Shopping Tab)
3. Upload an Excel file containing the scrape data
4. Click "Process File" to import the data

## License

[MIT License](LICENSE)