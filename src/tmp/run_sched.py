import fetchme
import logging
import utilityme as utils
import sched
import time


# --- set logging behaviour
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.getLogger('requests').setLevel(logging.ERROR)
logging.info('>> script started')

# PROCEDURE
# (1) schedule scihub search
# (2) if new product =>> gpt.schedule_interferogram_queue

# SCIHUB query
obj = fetchme.Scihub()
obj.query_auth('sebastien.valade', 'wave*worm')
conffile = '_config_ertaale.yml'
obj.read_configfile('./conf/' + conffile)

# productlist = obj.scihub_search(export_result=None, print_url=1)


# INTERVAL = 15  # every second


# s = sched.scheduler(time.time, time.sleep)
# while True:

#         productlist = obj.scihub_search(export_result=None, print_url=1)
#         s.enterabs(time_next_run, 1, < task_to_schedule_here > , < args_for_the_task > )
#     else:
#         time.sleep(INTERVAL)


# s = sched.scheduler(time.time, time.sleep)
# s.enter(INTERVAL, 1, obj.scihub_search(export_result=None), ())
# s.run()


# periodic_scheduler = utils.PeriodicScheduler()
# periodic_scheduler.setup(INTERVAL, obj.scihub_search(export_result=None, print_url=1))  # it executes the event just once
# periodic_scheduler.run()  # it starts the scheduler
