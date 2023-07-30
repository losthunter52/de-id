from celery import shared_task, current_task
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Semaphore
from anonymizer.utils.data_processing import value_to_dataframe
from anonymizer.lib.encryption import encrypt_aes, encrypt_chacha20, encrypt_salsa20
from anonymizer.lib.generalization import age_generalization, percent_generalization
from anonymizer.lib.hashing import apply_md5, apply_sha1, apply_sha256
from anonymizer.lib.masking import (
    mask_cpf, mask_email, mask_first_n_characters,
    mask_full, mask_last_n_characters, mask_range
)
from anonymizer.lib.null_out import drop_columns
from anonymizer.lib.perturbation import (
    perturb_date, perturb_numeric_gaussian,
    perturb_numeric_laplacian, perturb_numeric_range
)
from anonymizer.lib.pseudonymization import pseudonymize_columns, pseudonymize_rows
from anonymizer.lib.swapping import swap_columns, swap_rows
from .models import Task

# Dictionary mapping algorithm names to their corresponding functions
ALGORITHM_FUNCTIONS = {
    'encrypt.chacha20': encrypt_chacha20,
    'encrypt.aes': encrypt_aes,
    'encrypt.salsa20': encrypt_salsa20,
    'generalize.percent': percent_generalization,
    'generalize.age': age_generalization,
    'hash.md5': apply_md5,
    'hash.sha1': apply_sha1,
    'hash.sha256': apply_sha256,
    'mask_full': mask_full,
    'mask_range': mask_range,
    'mask_last_n_characters': mask_last_n_characters,
    'mask_first_n_characters': mask_first_n_characters,
    'mask_email': mask_email,
    'mask_cpf': mask_cpf,
    'drop_columns': drop_columns,
    'perturb_date': perturb_date,
    'perturb_numeric_range': perturb_numeric_range,
    'perturb_numeric_gaussian': perturb_numeric_gaussian,
    'perturb_numeric_laplacian': perturb_numeric_laplacian,
    'pseudonymize_columns': pseudonymize_columns,
    'pseudonymize_rows': pseudonymize_rows,
    'swap_columns': swap_columns,
    'swap_rows': swap_rows
}

@shared_task
def process_data(data: dict, user_pk: int):
    """
    Process the provided data using the specified algorithms and parameters.

    Args:
        data (dict): A dictionary containing 'data' and 'execution_parameters'.
                     'data' (list): List of dictionaries representing the input data.
                     'execution_parameters' (list): List of dictionaries containing the processing parameters.
                        Each dictionary contains the following keys:
                            - 'algorithm' (str): Name of the algorithm to apply.
                            - 'configuration' (dict): Algorithm-specific configuration parameters.
                            - 'columns' (dict): Column-specific configuration parameters.
        user_pk (int): The primary key of the user associated with this task.

    Return:
        dict: A dictionary containing the processed data and, if applicable, any encountered errors.
              The dictionary may have the following keys:
                - 'data' (list): List of dictionaries representing the processed data.
                - 'errors' (list): List of dictionaries containing error information.
                                   Each dictionary contains the following keys:
                                       - 'parameter_id' (int): ID of the parameter that caused the error.
                                       - 'algorithm' (str): Name of the algorithm that caused the error.
                                       - 'error_message' (str): Description of the encountered error.
    """
    # Create a new Task object to store the information about this task
    task_id = current_task.request.id

    task = Task.objects.create(task_id=task_id, user_id=user_pk, status='PROCESSING')
    task.save()

    # Extract execution parameters and input data from the provided data dictionary
    execution_parameters = data.get('execution_parameters', {})
    df = value_to_dataframe(data.get('data', []))
    errors = [] # List to store any errors that occur during processing

    semaphore = Semaphore()
    futures = []

    # Use ThreadPoolExecutor as a context manager to ensure proper cleanup
    with ThreadPoolExecutor() as executor:
        for parameter_id, parameter in enumerate(execution_parameters, start=1):
            algorithm = parameter.get('algorithm', {})
            configuration = parameter.get('configuration', {})
            columns = parameter.get('columns', {})
            future = executor.submit(
                apply_algorithm, algorithm, configuration, columns, df, semaphore, parameter_id, errors
            )
            futures.append(future)

    # Wait for all tasks to complete
    wait(futures)

    # Convert the processed DataFrame to a list of dictionaries
    processed_data = df.to_dict(orient='records')

    if errors:
        # If there are any errors, return them along with the processed data
        task.status = 'COMPLETED_WITH_ERRORS'
        task.result = processed_data
        task.errors = errors
        task.save()
    else:
        # If no errors, return just the processed data
        task.status = 'COMPLETED'
        task.result = task.result = processed_data
        task.save()
    
    return None

def apply_algorithm(algorithm: str, configuration: dict, columns: list, df, semaphore, parameter_id: int, errors: list):
    """
    Apply the specified algorithm to the DataFrame using the provided configuration and columns.

    Args:
        algorithm (str): Name of the algorithm to apply.
        configuration (dict): Algorithm-specific configuration parameters.
        columns (dict): Column-specific configuration parameters.
        df (pd.DataFrame): DataFrame containing the data to be processed.
        parameter_id (int): ID of the current processing parameter.

    Return:
        None
    """

    # Get the corresponding algorithm function based on the provided algorithm name
    algorithm_function = ALGORITHM_FUNCTIONS.get(algorithm)


    if algorithm_function:
        try:
            # Apply the algorithm function to the DataFrame with the provided configuration
            algorithm_function(df, columns, semaphore, **configuration)
        except ValueError as ve:
            # Catch specific exceptions and handle them appropriately
            error_message = str(ve)
        except Exception as e:
            # Catch any other unexpected exceptions
            error_message = f"Unexpected error: {str(e)}"
    else:
        # If the algorithm name is not valid, set an error message
        error_message = f"Invalid algorithm name: {algorithm}"

    if error_message:
        # If an error occurred, create an error info dictionary
        error_info = {
            "parameter_id": parameter_id,
            "algorithm": algorithm,
            "error_message": error_message
        }
        # Add the error info to the list of errors
        errors.append(error_info)

    # No return value
    return None
