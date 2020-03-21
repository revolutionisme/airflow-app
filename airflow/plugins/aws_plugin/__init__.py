from airflow.plugins_manager import AirflowPlugin
from aws_plugin.operators.s3_delete_objects import S3DeleteObjectsOperator


class AwsPlugin(AirflowPlugin):
    name = "aws_plugin"
    # A list of class(es) derived from BaseOperator
    operators = [S3DeleteObjectsOperator]
    # A list of class(es) derived from BaseHook
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []
