# ETL Backend

A FastAPI-based system for processing Payment and MTR reports, generating exemplar reports, and storing data in PostgreSQL.

## 🚀 Features

- FastAPI server with Swagger UI documentation
- Automated processing of Payment and MTR reports
- Generation of exemplar reports
- PostgreSQL database integration
- Detailed logging system
- Excel file transformation and processing

## 📋 Prerequisites

Before you begin, ensure you have the following installed:
- Python 3.8 or higher
- PostgreSQL
- Git

## 🔧 Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/shivxmr/interface-backend
   cd backend
   ```

2. **Create and activate virtual environment**
   ```bash
   # On Windows
   python -m venv venv
   .\venv\Scripts\activate

   # On macOS/Linux
   python -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure PostgreSQL**
   - Ensure PostgreSQL is running on your system
   - Create a new database for the project
   - Update the `DATABASE_URL` in `database.py` with your credentials:
     ```python
     DATABASE_URL = "postgresql://username:password@localhost:5432/database_name"
     ```

## 🏃‍♂️ Running the Application

1. **Initialize the database**
   ```bash
   python database.py
   ```
   This will create necessary tables and set up the database schema.

2. **Start the FastAPI server**
   ```bash
   python main.py
   ```
   The server will start running at `http://localhost:8000`

## 📝 Usage

1. **Access the API documentation**
   - Open your browser and navigate to `http://localhost:8000/docs`
   - This will open the Swagger UI interface

2. **Upload Reports**
   - In the Swagger UI, locate the `/upload/` endpoint
   - Click on "Try it out"
   - Upload both the Payment Report and MTR Report files
   - Click "Execute"

3. **Check Results**
   - The system will process your files and create:
     - A new exemplar report
     - Transformed payment report
     - Transformed MTR report
   - All output files will be saved in the `/output` folder
   - The exemplar report data will be automatically inserted into the PostgreSQL database

4. **Check Logs**
   - A `logs` folder will be created automatically
   - Contains detailed execution logs and any errors
   - Logs are organized by timestamp and type

## 📁 Project Structure

```
├── database.py          # Database configuration and models
├── main.py             # FastAPI application and routes
├── requirements.txt     # Project dependencies
├── logs/               # Generated log files
├── output/             # Generated output files
├── processing.py       # Processing code
└── README.md           # Project documentation
```

## 🔍 Monitoring

- Check the `logs` folder for detailed execution logs
- Common log files include:
  - Error logs
  - Processing logs
  - Database operation logs

## ⚠️ Troubleshooting

1. **Database Connection Issues**
   - Verify PostgreSQL is running
   - Check credentials in `database.py`
   - Ensure database exists and is accessible

2. **File Processing Errors**
   - Check input file formats
   - Verify file permissions
   - Review error logs in the `logs` folder

3. **Server Issues**
   - Verify port 8000 is available
   - Check if virtual environment is activated
   - Ensure all dependencies are installed