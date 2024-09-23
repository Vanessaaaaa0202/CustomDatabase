from flask import Flask, request, render_template
from db1 import CustomDatabase
from cc import cc
from myQuery import cc_sql
from MyRelationalDatabase import MyRelationalDatabase

app = Flask(__name__)
db = None
controller = None

if __name__ == '__main__':
    app.run(debug=True)

@app.route('/')  # homepage
def index():
    return render_template('index.html')

@app.route('/execute', methods=['POST'])
def execute():
    global db
    global controller

    if controller is None:
        # Initialize controller if not already initialized
        db_choice = request.form.get('database_choice')
        if db_choice == 'JSON':
            db = CustomDatabase()
            controller = cc()
        elif db_choice == 'SQL':
            db = MyRelationalDatabase()
            controller = cc_sql()

    # Get the command from the form data
    user_input = request.form.get('command')
    limit = request.form.get('limit')
    condition = request.form.get('condition')
    columns = request.form.get('columns')

    # Call the method of the cc class that executes the command
    result = controller.callkaidi(user_input, limit, condition, columns)

    if result:
        # Render the results.html template with the result
        return render_template('results.html', result=result)
    else:
        return render_template('results.html', result='Command execution failed'), 400

@app.route('/switch_db', methods=['POST'])
def switch_db():
    global controller
    db_choice = request.form.get('database_choice')

    if db_choice == 'JSON':
        controller = cc()
    elif db_choice == 'SQL':
        controller = cc_sql()

    print(f"controller in switch_db: {controller}")

    return render_template('interface.html')
