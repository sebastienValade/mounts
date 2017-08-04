import logging
import snapme as gpt

# --- set logging behaviour
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.getLogger('requests').setLevel(logging.ERROR)
logging.info('>> script started')


config_file = './conf/config_processing.yml'
gpt.graph_processing(config_file)
