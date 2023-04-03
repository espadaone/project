# Copyright 2020, Brigham Young University-Idaho. All rights reserved.

import etlfunctionfill
from etlfunctionfill import transform_data
from etlfunctionfill import fill_empty_values
import pytest
from pytest import approx



import pandas as pd

def test_transform_data():
    """Verify that the filter_for_meter
    function works correctly.
    """
    df = pd.DataFrame({
        'row_id': [1, 2, 3, 4, 4],
        'subject_id': [1, 2, 3, 4, 4],
        'startdate': ["2010-01-01",'NaT',"2010-01-01","2010-01-01","2010-01-01"],
        'enddate': ["2010-01-01",'NaT',"2010-01-01","2010-01-01","2010-01-01"],
        'col_1': [1, 2, 3, 4, 4],
        'col_2': ['a', 'b', 'c', 'd', 'd']
    })
    
    # Aplica la función sobre el dataframe de prueba
    result = transform_data(df)
    
    # Define el dataframe esperado después de remover las filas duplicadas
    expected = pd.DataFrame({
        'prescriptions_id': [1,2,3,4],
        'startdate': ["2010-01-01","1970-01-01","2010-01-01","2010-01-01"],
        'enddate': ["2010-01-01","1970-01-01","2010-01-01","2010-01-01"],
        'col_1': [1, 2, 3, 4],
        'col_2': ['a', 'b','c','d']
    })
    
    # Compara el resultado de la función con el dataframe esperado
    assert result.equals(expected)

def test_fill_empty_values():
    """Verify that the filter_for_meter
    function works correctly.
    """
    dfe = pd.DataFrame({
        'prescriptions_id': [1,2,3,4,5],
        'hadm_id': [1, 2, 3, 4, 5], 
        'icustay_id': [1, 2, 3, 4, 5],
        'startdate': ['a', 'b','c' ,'d','e'],
        'enddate': ['a','b','c','d','e'], 
        'drug_type': ['a', 'b','c','d','e'], 
        'drug': ['a', 'b','c','d','e'], 
        'drug_name_poe': ['a','b',None,None,'e'],
        'drug_name_generic': ['a',None,'c','d','e'],
        'formulary_drug_cd': [None,'b','c','d','e'],
        'gsn': [1, 2, 3, 4, 5],
        'ndc': [1, 2, 3, 4, 5],
        'prod_strength': ['a', 'b','c','d','e'], 
        'dose_val_rx': ['a','b','c','d','e'], 
        'dose_unit_rx': ['a','b','c','d','e'], 
        'form_val_disp': ['a', 'b','c','d','e'],
        'form_unit_disp': ['a','b','c','d','e'],
        'route': ['a', 'b','c','d','e'] 
            
    })
    
    # Aplica la función sobre el dataframe de prueba
    result = fill_empty_values(dfe)
    
    # Define el dataframe esperado después de remover las filas duplicadas
    expected = pd.DataFrame({
        'prescriptions_id': [1,2,3,4,5],
        'hadm_id': [1, 2, 3, 4, 5], 
        'icustay_id': [1, 2, 3, 4, 5],
        'startdate': ['a', 'b','c','d','e'],
        'enddate': ['a','b','c','d','e'], 
        'drug_type': ['a', 'b','c','d','e'], 
        'drug': ['a', 'b','c','d','e'], 
        'drug_name_poe': ['a','b','N/D','N/D','e'],
        'drug_name_generic': ['a','N/D','c','d','e'],
        'formulary_drug_cd': ['N/D','b','c','d','e'],
        'gsn': [1, 2, 3, 4, 5],
        'ndc': [1, 2, 3, 4, 5],
        'prod_strength': ['a', 'b','c','d','e'], 
        'dose_val_rx': ['a','b','c','d','e'], 
        'dose_unit_rx': ['a','b','c','d','e'], 
        'form_val_disp': ['a', 'b','c','d','e'],
        'form_unit_disp': ['a','b','c','d','e'],
        'route': ['a', 'b','c','d','e']        
    })
    
    # Compara el resultado de la función con el dataframe esperado
    assert result.equals(expected)


# Call the main function that is part of pytest so that the
# computer will execute the test functions in this file.
pytest.main(["-v", "--tb=line", "-rN", __file__])
