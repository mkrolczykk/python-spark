Steps to generate input data for your project:
1. Copy the `bigdata-input-generator` folder in the root directory of your exam repo
2. Install the required python packages and run the main script from the root directory of the exam repo 
   to generate the datasets (you can use `virtualenv` for this step):
```bash
pip install -r bigdata-input-generator/requirements.txt
python bigdata-input-generator/main.py
```
3. Add `capstone-dataset` (generated folder) to `.gitignore` in your exam repo
