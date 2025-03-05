'''Module for Flask enpoints that Apache Airflow tasks to call.'''

import os
from datetime import datetime

from flask import Flask, jsonify, request  # type: ignore

from core import query_composition, constants, utils, request_helpers

app = Flask(__name__)

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    utils.logger.info("API status check called")
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': constants.SERVICE_NAME
    }), 200

@app.route('/fix_loop_variables', methods=['POST'])
def fix_loop_variables():
    mapping: dict[str, any] = request.get_json() or {}
    source, destination = request_helpers.extract_source_and_destination(mapping)
    
    try:
        utils.logger.info(f"fix_loop_variables endpoint called. Generating {source} from {destination}.")
        status = query_composition.compose_coalesce_loop_variable_query(source, destination)
        return jsonify({
            'status': status,
            'timestamp': datetime.utcnow().isoformat(),
            'service': constants.SERVICE_NAME
        }), 200
    except Exception as e:
        utils.logger.exception("An error occurred in fix_loop_variables endpoint.")
        return jsonify({'error': 'Internal Server Error', 'message': str(e)}), 500

@app.route('/merge_table_versions', methods=['POST'])
def merge_table_versions():
    mapping: dict[str, any] = request.get_json() or {}
    source, destination = request_helpers.extract_source_and_destination(mapping)
    
    try:
        utils.logger.info(f"merge_table_versions endpoint called. Merging {source} to generate {destination}.")
        status = query_composition.create_or_replace_table_with_outer_join(source, destination)
        return jsonify({
            'status': status,
            'timestamp': datetime.utcnow().isoformat(),
            'service': constants.SERVICE_NAME
        }), 200
    except Exception as e:
        utils.logger.exception("An error occurred in merge_table_versions endpoint.")
        return jsonify({'error': 'Internal Server Error', 'message': str(e)}), 500
