from airflow.hooks.base_hook import BaseHook

class SodaHook(BaseHook):
    def __init__(
            self,
            dataset_id,
            *args,
            **kwargs
    ):
        """
        Borrowed from airflow.contrib
        Create new connection to Socrata Open Data API
        and allows you to pull data out of SODA and save it to a file.
        You can then use that file with other
        Airflow operators to move the data into another data source
        :param dataset_id:  the unique id of the dataset to access.`
        """
        self.url = None
        self.dataset_id = dataset_id
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson

    def get_url(self):
        """
        Create the url for a specific dataset
        if we already have the url created for a particular
        dataset, just return that url
        """
        if self.url:
            return self.url

        base_url = 'https://data.cityofnewyork.us/resource/'
        url = base_url + str(self.dataset_id) + '.json?'
        self.url = url
        return self.url
