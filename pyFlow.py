# PyFlow - Control Framework for Python workflows
# Will provide table level logging of Python processes

# Available functions:
# get_next_run_id() - returns next value from RUN_ID sequence
# get_flow_info(flow_id) - returns all the flow information formatted as flow object for a given RUN_ID
# get_load_dt(date_type:int[1,2]) - returns current date formatted
# log_to_db(caller_name, log_level, log_message) - writes a record out to PYFLOW_LOG table
# log_info(caller_name, log_message) - writes an INFO level record to PYFLOW_LOG
# log_warn(caller_name, log_message) - writes an WARNING level record to PYFLOW_LOG
# log_error(caller_name, log_message) - writes an ERROR level record to PYFLOW_LOG

# Version history
# 0.1 20220622  Mark Regan
# 0.2 20240818  Tomasz

# Required libraries
from sqlalchemy import create_engine, MetaData, Table, update, text
from sqlalchemy.orm import sessionmaker
import os
import datetime

class Flow:
    def __init__(self, flow_info):
        self.id = flow_info[1]
        self.name = flow_info[2]
        self.description = flow_info[3]
        self.created_by = flow_info[4]
        self.modified_by = flow_info[5]

        self.run_id = 0
        self.run_steps = ()

_log = None
process_name = None

def init_log(logger=None):
    global _log

    if logger != None:
        _log = logger
    else:
        import logging
        _log = logging.getLogger()

def init_flow(proc_name):
    global process_name
    process_name = proc_name

    _log.info('Initialising Feed - ' + process_name)
    try:
        flow_info = get_flow_info(process_name.upper())
        if flow_info[0] == 0:
            this = Flow(flow_info)
            flow_id = this.id
            _log.info('Identified Flow Name - ' + process_name +
                      ' with Flow ID - ' + str(flow_id))
            this = create_flow_run(this)
            start_log(process_name, this.run_id)

        return this
    except:
        _log.error('Could not initialise flow for process name - ' + process_name)
        SystemExit(1)

def start_log(process_name, run_id):

    _log.info('#####################################')
    _log.info('PROCESS: ' + process_name)
    _log.info('STARTED AT: ' +
              datetime.datetime.now().strftime("%Y%m%d %H%M%S"))
    _log.info('RUN ID: ' + str(run_id))
    _log.info('#####################################')
    _log.info(' ')

def create_connection(db):
    connection = create_engine(f'postgresql://{db.user}:{db.pwd}@{db.db_host}:{db.db_port}/{db.db_name}')
    return connection

def end_flow(run_id):

    sql = 'update pyflow_log set end_dt = :new_end_dt where run_id = :new_run_id'
#{'new_end_dt': datetime.datetime.now(), 'new_run_id': str(run_id)})
    try:
        engine = create_connection(db)
        Session = sessionmaker(bind=engine)
        session = Session()
        session.execute(text(sql), {"new_end_dt": datetime.now(), "new_run_id": run_id})
        session.commit()
    except cx_Oracle.Error as error:
        print(error)
        raise SystemExit(5)
    
def create_flow_run(flow):

    _log.info('Recording Flow Run - ' + flow.name)
    try:
        run_id = get_next_run_id()
        _log.info('Retrieved next run id - ' + str(run_id))
        flow.run_id = run_id
        try:
            _log.info('Creating run entry for Flow ID - ' + str(flow.id))
            create_run_record(flow.id, flow.run_id)
        except:
            _log.error('Failed to create run entry for Flow ID - ' + str(flow.id))
        return flow
    except:
        _log.error('Could not retrieve next run id')
        raise SystemExit(5)

def run_steps(flow):

    for step in flow.run_steps:
        step_type = step[3]
        step_name = step[1]
        step_desc = step[2]
        step_params = step[4]

        if step_type.upper() == 'PYTHON SCRIPT':
            exec_python_script(step_params)
        elif step_type.upper() == 'SQL SCRIPT':
            exec_sql_script(step_params)

def exec_sql_script(params):
    return True

def exex_python_script(params):
    return True

def get_flow_steps(flow):
    

        
