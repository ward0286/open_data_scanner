"""Contains Inventory class."""

import concurrent.futures
from dataclasses import dataclass, field
import datetime as dt
import re
import threading
import time
from tqdm import tqdm
from typing import Any, Dict, List, Optional, NoReturn
import urllib3
import validators
import warnings

import pandas as pd
from colorama import Fore, init

from .constants import * # pylint: disable=import-error
from .data import ISO639_MAP, FORMATS
from .tools import TenaciousSession, DataCatalogue, DriverDataCatalogue
from .helper_functions import * # pylint: disable=import-error

@dataclass
class Inventory:
    """Keeps track of datasets' and resources' information in two respective 
    dataframes. Contains both static and instance methods to parse given 
    registries and auto-complete columns.
    """

    datasets: pd.DataFrame = field(
        default_factory=lambda: (pd.DataFrame(columns=DATASETS_COLS)
                                 .astype(DATASETS_DTYPES))
    )
    """DataFrame storing the datasets' information."""

    resources: pd.DataFrame = field(
        default_factory=lambda: (pd.DataFrame(columns=RESOURCES_COLS)
                                 .astype(RESOURCES_DTYPES))
    )
    """DataFrame storing the resources' information."""


    @staticmethod
    def add_dataset(dataset: dict, datasets: pd.DataFrame,
                lock: threading.Lock) -> NoReturn:
        """Adds the given dataset's information to the datasets dataframe.
        The lock argument is a mutex on the datasets dataframe."""

        try:
            record: Dict[str, Any] = {}
            record['id'] = dataset['id']
            record['title_en'] = dataset['title_translated']['en']
            record['title_fr'] = dataset['title_translated']['fr']
            record['published'] = dt.datetime.strptime(
                dataset['date_published'],'%Y-%m-%d %H:%M:%S').isoformat()
            record['metadata_created'] = dataset['metadata_created']
            record['metadata_modified'] = dataset['metadata_modified']
            record['num_resources'] = dataset['num_resources']

            # metadata specific to each platform
            org = dataset['organization']['name']
            org_title = re.sub(
                r'([^\|]+) \| ([^\|]+)', r'\1', dataset['organization']['title']
            )
            
            record['on_registry'] = True
            record['org'] = org
            record['org_title'] = org_title
            record['registry_link'] = REGISTRY_DATASETS_BASE_URL.format(
                record['id'])
            record['harvested'] = False
            record['internal'] = False

            # inconsistent metadata fields

            if dataset['maintainer_email'] is not None:
                record['maintainer_email'] = dataset['maintainer_email'].lower()
            elif 'data_steward_email' in dataset.keys() and \
                    dataset['data_steward_email'] is not None:
                record['maintainer_email'] = dataset['data_steward_email'].lower()
            elif 'author_email' in dataset.keys() and \
                    dataset['author_email'] is not None:
                record['maintainer_email'] = dataset['author_email'].lower()
            else:
                record['maintainer_email'] = None
            record['maintainer_name'] = infer_name_from_email(
                record['maintainer_email'])

            try:
                record['collection'] = dataset['collection']
            except KeyError:
                record['collection'] = None

            record['frequency'] = dataset['frequency']
            if not isinstance(record['frequency'], str):
                print(f"Error for id {record['id']}:",
      f"frequency is {record['frequency']} (not str)")


            # 'modified', 'up_to_date', 'official_lang', 'open_formats' and 'spec' 
            # will be added to the record later on

        except Exception as e: # pylint: disable=bare-except
            print(f'!!! An exception occurred in add_dataset:\n{e}')

        lock.acquire()
        datasets.loc[len(datasets)] = record # type: ignore
        lock.release()

    @staticmethod
    def add_resource(resource: dict, resources: pd.DataFrame, 
                    lock: threading.Lock) -> NoReturn:
        """Inserts the given resource's information in the resources dataframe."""
        try:
            record: Dict[str, Any] = {
            # Required fields with defaults
            'id': resource.get('id', ''),
            'title_en': resource.get('name', ''),
            'created': resource.get('created', ''),
            'format': resource.get('format', ''),
            'dataset_id': resource.get('package_id', ''),
            'resource_type': resource.get('resource_type', ''),
            'url': resource.get('url', ''),
            'title_fr': '',
            'metadata_modified': '',
            'lang': '',
            'url_status': -1,
            'https': False,
            }

            # URL validation
            if record['url']:
                record['https'] = str(record['url']).startswith('https') or \
                    str(record['url']).startswith('file')
                if validators.url(record['url']):
                    urllib3.disable_warnings(
                        urllib3.exceptions.InsecureRequestWarning
                    )
                    record['url_status'] = TenaciousSession(
                        skip_ssl=True
                    ).get_status_code(record['url'])

            # Handle languages safely
            if 'language' in resource:
                try:
                    lang: List[str] = [ISO639_MAP.get(x, x) for x in resource['language']]
                    record['lang'] = '/'.join(lang)
                except:
                    record['lang'] = ''

            # Optional metadata
            if 'metadata_modified' in resource:
                record['metadata_modified'] = resource['metadata_modified']

            # Handle different title translation formats
            name_translated = resource.get('name_translated', {})
            if isinstance(name_translated, dict):
                record['title_fr'] = name_translated.get('fr') or \
                                name_translated.get('fr-t-en', '')

            # Add registry link
            record['registry_link'] = REGISTRY_RESOURCES_BASE_URL.format(
                record['dataset_id'], record['id'])

        except Exception as e:
            # print(f'!!! An exception occurred in add_resource:\n{e}')
            return

        lock.acquire()
        resources.loc[len(resources)] = record
        lock.release()

    @staticmethod
    def infer_modified(ds: pd.Series, resources: pd.DataFrame) -> dt.datetime:
        """Infers dataset's last modification date based on metadata and resources."""
        modified_dates = []
    
        # Add dataset metadata date if exists
        if ds.metadata_modified:
            modified_dates.append(ds.metadata_modified)
        
        # Add resource dates - check different possible column names
        ds_resources = resources[resources.dataset_id == ds.id]
        date_columns = ['last_modified', 'metadata_modified', 'created']
    
        for col in date_columns:
            if col in ds_resources.columns:
                resource_dates = ds_resources[col].dropna().tolist()
                modified_dates.extend(resource_dates)
                break
    
        # Return latest date or fallback
        if modified_dates:
            return max(modified_dates)
        else:
            return ds.metadata_modified or dt.datetime.now()

    @staticmethod
    def get_up_to_date(ds: pd.Series,
                       now: dt.datetime = dt.datetime.now()) -> bool:
        """Computes currency based on the frequency and last date modified of 
        the dataset. Returns True if dataset is up to date and False if it 
        needs update or cannot read the frequency. (Note: readable frequencies 
        are stored in formats as P1D, P3W, P6M, P1Y, etc.)
        """
        # returning True (up to date) if the dataset is harvested
        if ds.harvested:
            return True
        # computing oldest date considered as valid to be up to date
        frequency: str = ds.frequency
        if isinstance(frequency, str) and frequency.startswith('P') and \
                frequency != 'PT1S':
            full_unit: Dict[str, str] = {'D': 'day', 'W': 'week', 
                                        'M': 'month', 'Y': 'year'}
            unit: str = full_unit[frequency[-1]]
            n: float = float(frequency[1:-1]) # e.g. 1, 2, 0.5 ...
            oldest_valid_update: dt.datetime = date_ago(n, unit, from_=now)
        else:
            # no update explicitly planned
            return True

        # Getting last modified date, based on resources
        last_modified = dt.datetime.fromisoformat(ds.modified)
        if last_modified >= oldest_valid_update:
            return True
        else:
            return False

    @staticmethod
    def get_official_lang(ds: pd.Series, all_resources: pd.DataFrame) -> bool:
        """Returns True if dataset ds is compliant with official languages 
        requirements; false otherwise (i.e. checks whether there are as many 
        resources in English as there are in French).
        """
        resources = all_resources[all_resources.dataset_id == ds.id]
        num_eng: int = 0
        num_fra: int = 0
        for _, res in resources.iterrows():
            if 'eng' in res.lang:
                num_eng += 1
            if 'fra' in res.lang:
                num_fra += 1
        return num_eng == num_fra

    @staticmethod
    def get_open_formats(ds: pd.Series, all_resources: pd.DataFrame) -> bool:
        """Returns True if dataset ds is compliant with open formats 
        requirements; false otherwise (i.e. checks for each format type, 
        assuming each format type contains the same data, that at least one
        type is open).
        """
        resources = all_resources[all_resources.dataset_id == ds.id].copy()
        # FORMATS = pd.read_csv('./helper_tables/formats.csv')
        resources = resources.merge(FORMATS, how='left', on='format')
        for elem in resources.groupby('format_type').open.unique():
            if True not in elem:
                # if a format type doesn't have a single resource in an open format
                return False

        return True

    @staticmethod
    def get_spec(ds: pd.Series, all_resources: pd.DataFrame) -> bool:
        """Returns True if dataset ds is compliant with specification / data 
        dictionary requirements; false otherwise (i.e. if the dataset has 1+ 
        resource classed as 'dataset', checks if it also has resources whose title 
        include "data dictionary" or "specification", or with a title that starts 
        or ends with "dd_" / "_dd" respectively; if not, is non-compliant).
        """
        warnings.filterwarnings('ignore', category=UserWarning)
        resources = all_resources[all_resources.dataset_id == ds.id].copy()
        if 'dataset' in list(resources.resource_type):
            if resources['title_en'].str.contains(
                    r'(data dictionary|specification|^dd[_\-]|[_\-]dd.)', 
                    case=False).sum():      
                return True
            return False
        # no dataset => no need for data dictionary/specification
        return True

    # In inventories.py, modify _collect_dataset_with_resources:

    def _collect_dataset_with_resources(
        self, dc: DataCatalogue, id: str,
        datasets_lock: threading.Lock,
        resources_lock: threading.Lock,
        driver_lock: Optional[threading.Lock] = None,
        pbar: Optional[tqdm] = None) -> NoReturn:
        """Fetches dataset and resource information from the DataCatalogue"""
        try:
            if driver_lock:
                driver_lock.acquire()
            dataset: dict = dc.get_dataset(id)
            if driver_lock:
                driver_lock.release()

            # adds dataset to the common dataframe
            Inventory.add_dataset(
                dataset, self.datasets, datasets_lock)
    
            for resource in dataset['resources']:
                # adds resource to the common dataframe
                Inventory.add_resource(
                    resource, self.resources, resources_lock)
    
        except Exception as e:
            print(f"\nFailed to fetch dataset {id}: {str(e)}")
        finally:
            if pbar:
                pbar.update()

    def inventory(self, dc: DataCatalogue,
             datasets_ids: Optional[List[str]] = None,
             owner_org: Optional[str] = None) -> NoReturn:
        """Fetches information of all datasets and resources of the given 
        DataCatalogue dc and stores it in self datasets and resources 
        dataframes, in parallel.

        Args:
            dc: DataCatalogue instance to fetch from
            datasets_ids: Optional list of dataset IDs to process
            owner_org: Organization ID to fetch datasets for if no IDs provided
        """
        print()
        print('Collecting information of all datasets ...')
        start = time.time() # times datasets collection

        if not datasets_ids:
            # listing all the datasets IDs for the specified org:
            datasets_ids = dc.search_datasets(owner_org=owner_org)
        
        # initializing the progress bar
        pbar = tqdm(desc='Processed Datasets', total=len(datasets_ids),
                colour='green', ncols=100, ascii=' -=')

        # in parallel threads, collects relevant information of
        # each dataset and associated resources
        datasets_lock = threading.Lock()
        resources_ids_lock = threading.Lock()
        driver_lock = None
        if isinstance(dc, DriverDataCatalogue):
            driver_lock = threading.Lock()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for id in datasets_ids:
                executor.submit(self._collect_dataset_with_resources, dc, id, 
                                datasets_lock, resources_ids_lock, 
                                driver_lock, pbar)
            executor.shutdown(wait=True)
        pbar.close()
        end = time.time() # ends datasets collection timer

        self.datasets = (self.datasets
                         .sort_values(by='id')
                         .reset_index(drop=True)
                         .astype(DATASETS_DTYPES)
        )
        self.resources = (self.resources
                          .sort_values(by='dataset_id')
                          .reset_index(drop=True)
        )
        print(f'All information was collected.  ({end-start:.2f}s)')


    def complete_modified(self) -> NoReturn:
        """Completes column 'modified' of the datasets table."""
        self.datasets.modified = self.datasets.apply(
            lambda ds: Inventory.infer_modified(ds, self.resources), axis=1
        )

    def complete_up_to_date(self, 
                            now: dt.datetime = dt.datetime.now()) -> NoReturn:
        """Completes 'up_to_date' column of the datasets table."""
        self.datasets.up_to_date = self.datasets.apply(
            lambda ds: Inventory.get_up_to_date(ds, now), axis=1
        )

    def complete_official_lang(self) -> NoReturn:
        """Completes 'official_lang' column of the datasets table."""
        self.datasets.official_lang = self.datasets.apply(
            lambda ds: Inventory.get_official_lang(ds, self.resources), axis=1
        )

    def complete_open_formats(self) -> NoReturn:
        """Completes 'open_formats' column of the datasets table."""
        self.datasets.open_formats = self.datasets.apply(
            lambda ds: Inventory.get_open_formats(ds, self.resources), axis=1
        )

    def complete_spec(self) -> NoReturn:
        """Completes 'spec' column of the datasets table."""
        self.datasets.spec = self.datasets.apply(
            lambda ds: Inventory.get_spec(ds, self.resources), axis=1
        )

    def complete_missing_fields(self) -> NoReturn:
        """Completes columns of datasets inventory: 'modified', 'up_to_date', 
        'official_lang', 'open_formats' and 'spec' (details given in getters 
        documentation).
        """
        # Computing modified for datasets
        print()
        print('Completing datasets\' modified dates.')
        self.complete_modified()
        # Checking currency for datasets
        print('Verifying datasets\' currency.')
        self.complete_up_to_date()
        # Checking official languages compliancy
        print('Verifying official languages compliance.')
        self.complete_official_lang()
        # Checking open formats compliancy
        print('Verifying open formats compliance.')
        self.complete_open_formats()
        # Checking specification
        print('Verifying specification / data dictionary compliance.')
        self.complete_spec()
        print("Inventories are ready.")

    def update_platform_info(self, platform: str, dc: DataCatalogue,
                             id_list: Optional[List[str]] = None) -> NoReturn:
        """Updates the registry or catalogue info (platform passed in the 
        arguments) of the datasets whose id is in the given list, along
        with the platform links of their associated resources. If no list 
        given, checks all the datasets.
        """
        if not id_list:
            id_list = list(self.datasets.id)

        pbar = tqdm(desc='Processed Datasets', total=len(id_list), 
                    colour='green', ncols=100, ascii=' -=') 

        match platform:
            case 'registry':
                cols_to_update = ['on_registry', 'org',
                                  'org_title', 'registry_link']
                datasets_base_url = REGISTRY_DATASETS_BASE_URL
                resources_base_url = REGISTRY_RESOURCES_BASE_URL
            case 'catalogue':
                cols_to_update = ['on_catalogue', 'aafc_org', 
                                  'aafc_org_title', 'catalogue_link']
                datasets_base_url = CATALOGUE_DATASETS_BASE_URL
                resources_base_url = CATALOGUE_RESOURCES_BASE_URL
            case _:
                raise ValueError('platform parameter must be either'
                                 ' "registry" or "catalogue"')

        for id in id_list:
            try:
                dataset = dc.get_dataset(id)

                # update dataset
                org = dataset['organization']['name']
                org_title = re.sub(r'([^\|]+) \| ([^\|]+)', r'\1',
                                   dataset['organization']['title'])
                link = datasets_base_url.format(id)
                self.datasets.loc[self.datasets.id == id, 
                                  cols_to_update] = True, org, org_title, link

                # update resources links
                resources_ids = [res['id'] for res in dataset['resources']]
                self.resources[cols_to_update[-1]] = self.resources.apply(
                    lambda row: (resources_base_url
                                 .format(id, row.id) if row.id in resources_ids # pylint: disable=cell-var-from-loop
                                 else row.catalogue_link), axis=1)
            except: # pylint: disable=bare-except
                pass
            finally:
                pbar.update()


    def export_datasets(self, path: str = './', filename: str = '') -> NoReturn:
        """Exports self datasets dataframe as a csv file at the given path, if
        any; if none given, exports it in the current folder.
        """
        self._export_to_csv(self.datasets, 'datasets', path, filename)

    def export_resources(self, path: str = './', filename: str = '') -> NoReturn:
        """Exports self resources dataframe as a csv file at the given path, if
        any; if none given, exports it in the current folder.
        """
        self._export_to_csv(self.resources, 'resources', path, filename)

    def _export_to_csv(self, df: pd.DataFrame, df_name: str, 
                       path: str, filename: str) -> NoReturn:
        """Exports DataFrame df as a csv file to the given path, if any. 
        Needs also the name of df as a string for outputs.
        """

        # makes sure there is no backslash issue in path name
        if path != './':
            path = re.sub(r'[\\]+', '/', path)
            if not path.endswith('/'):
                path = path + '/'
        # makes sure full_path exists; creates directories if needed
        check_and_create_path(path)
        if filename == '':
            timestamp: str = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
            filename = f'{timestamp}_{df_name}_inventory.csv'
        full_path: str = path + filename
        msg: str
        init()
        try:
            df.to_csv(full_path, index=False, encoding='utf_8_sig')
        except Exception as e: # pylint: disable=bare-except
            msg = f'Error exporting {df_name} inventory to {filename}:\n{e}\n'
            print(Fore.RED + msg + Fore.RESET)
        else:
            msg = (
    f"{df_name.capitalize()} inventory was successfully exported to {filename}."
)
            print(Fore.GREEN + msg + Fore.RESET)