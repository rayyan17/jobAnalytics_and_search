from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators


class CopyToRedshiftPlugin(AirflowPlugin):
    name = "copy_redshift_plugin"
    operators = [
    	operators.CopyToRedshiftOperator,
    	operators.DataQualityOperator
    ]
