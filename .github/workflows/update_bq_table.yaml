name: Update BQ Table

on:
  workflow_dispatch:

jobs:
  run-job:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Create JSON credentials file
        uses: jsdaniell/create-json@v1.2.3
        with:
          name: "gcloud-service-key.json"
          json: ${{ secrets.GCP_SERVICE_ACCOUNT_KEY }}

      - name: Set up gcloud CLI
        uses: google-github-actions/setup-gcloud@v1
        with:
          project_id: ${{ secrets.GCP_PROJECT_ID }}
          export_default_credentials: false

      - name: Authenticate to Google Cloud and set ADC
        run: |
          gcloud auth activate-service-account --key-file=gcloud-service-key.json
          gcloud config set project ${{ secrets.GCP_PROJECT_ID }}
          gcloud config set run/region ${{ secrets.GCP_REGION }}
          echo "GOOGLE_APPLICATION_CREDENTIALS=gcloud-service-key.json" >> $GITHUB_ENV

      - name: Install jq
        run: sudo apt-get install -y jq

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install Python dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r bq_scripts/requirements.txt

      - name: Run BigQuery Upsert Pipeline
        run: |
          python bq_scripts/bq_main.py \
            --project_id="${{ secrets.GCP_PROJECT_ID }}" \
            --dataset_id="$(jq -r '.dataset_id' bq_config.json)" \
            --key_columns_json="$(jq -c '.key_columns' silver_layer/validation_config.json)"
