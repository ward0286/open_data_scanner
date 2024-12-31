"""Parses AAFC's open data on Canada's Open Government Portal (as well as the 
departmental AAFC Open Data Catalogue in a further version), to provide the 
user with a complete inventory of datasets and resources in csv files.
"""

import atexit
import msvcrt
import pandas as pd
from fuzzywuzzy import process
from typing import List, NoReturn, Dict
import warnings
from colorama import Fore

from .constants import REGISTRY_BASE_URL
from .tools import RequestsDataCatalogue
from .inventories import Inventory


warnings.filterwarnings('ignore', category=FutureWarning)
def get_organizations(registry: RequestsDataCatalogue) -> Dict[str, str]:
    """Gets all organizations from the portal"""
    orgs = registry.get_organizations()
    return {org['id']: org['title'] for org in orgs}

@atexit.register
def display_exit_message() -> NoReturn:
    """Asks user to click enter when program ends"""
    print(Fore.CYAN + '\nClick Enter to exit.' + Fore.RESET)
    input()

def get_org_selection(organizations: Dict[str, str]) -> tuple[str, str]:
    """Interactive organization selection with fuzzy search"""
    while True:
        print(Fore.CYAN + '\nStart typing organization name (or "q" to quit):' + Fore.RESET, end=" ")
        search = input().strip()
        
        if search.lower() == 'q':
            return None, None
            
        if len(search) < 2:
            print(Fore.YELLOW + "Please type at least 2 characters" + Fore.RESET)
            continue
            
        # Get fuzzy matches
        matches = process.extract(search, 
                                choices=organizations.values(),  # Search only in org names
                                limit=5)
        
        if not matches:
            print(Fore.RED + "No matching organizations found" + Fore.RESET)
            continue
            
        # Display matches
        print("\nMatching organizations:")
        for idx, match in enumerate(matches, 1):
            org_name = match[0]  # First element is the org name
            score = match[1]     # Second element is the match score
            print(f"{idx}. {org_name} (Match: {score}%)")
            
        # Let user select from matches
        try:
            print(Fore.CYAN + "\nSelect number (or press Enter to search again):" + Fore.RESET, end=" ")
            selection = input().strip()
            if not selection:
                continue
                
            idx = int(selection) - 1
            if 0 <= idx < len(matches):
                selected_name = matches[idx][0]
                selected_id = [k for k,v in organizations.items() 
                             if v == selected_name][0]
                return selected_id, selected_name
                
            print(Fore.RED + "Invalid selection" + Fore.RESET)
            
        except ValueError:
            print(Fore.RED + "Invalid selection. Please enter a valid number" + Fore.RESET)

def main() -> NoReturn:
    """Main code."""
    print()
    print(Fore.YELLOW + '\tOpen Government Data Scanner' + Fore.RESET)
    print('\nScanning for available organizations...')

    # Get organizations
    registry = RequestsDataCatalogue(REGISTRY_BASE_URL)
    organizations = get_organizations(registry)

    # Get user selection using fuzzy search
    selected_org_id, selected_org_name = get_org_selection(organizations)
    if not selected_org_id:
        sys.exit()

    print(f'\nScanning datasets for: {selected_org_name}')
    inventory = Inventory()

    # Scan registry for selected organization
    registry_datasets: List[str] = registry.search_datasets(owner_org=selected_org_id)
    # print(registry_datasets)
    print(Fore.GREEN)
    print(f'{len(registry_datasets)} datasets were found on the registry.' + Fore.RESET)
    inventory.inventory(registry, registry_datasets, selected_org_id)

    # Announce total counts
    print()
    print(Fore.YELLOW + f'{len(inventory.datasets)}' + Fore.RESET,
          'datasets and',
          Fore.YELLOW + f'{len(inventory.resources)}' + Fore.RESET,
          'resources were found.')

    # Complete missing fields
    inventory.complete_missing_fields()
    
    # Fill empty fields with selected org info
    inventory.datasets = inventory.datasets.fillna({
        'on_registry': True,
        'org': selected_org_id,
        'org_title': selected_org_name
    })

    # Create safe filename from org name (replace spaces/special chars with underscore)
    safe_org_name = selected_org_name.lower().replace(' ', '_')
    safe_org_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe_org_name)

    # Export inventories
    print('\nSaving inventories...')
    inventory.export_datasets(path='./inventories/',
                            filename=f'{safe_org_name}_datasets_inventory.csv')
    inventory.export_resources(path='./inventories/',
                             filename=f'{safe_org_name}_resources_inventory.csv')
    
    datasets_file = f'./inventories/{safe_org_name}_datasets_inventory.csv'
    df = pd.read_csv(datasets_file)
    columns_to_remove = [
        'on_catalogue',
        'aafc_org',
        'aafc_org_title',
        'harvested',
        'internal',
        'catalogue_link'
    ]
    df = df.drop(columns=columns_to_remove, errors='ignore')
    df.to_csv(datasets_file, index=False)

    resources_file = f'./inventories/{safe_org_name}_resources_inventory.csv'
    df = pd.read_csv(resources_file)
    columns_to_remove = [
        'catalogue_link'
    ]
    df = df.drop(columns=columns_to_remove, errors='ignore')
    df.to_csv(resources_file, index=False)

if __name__ == '__main__':
    main()