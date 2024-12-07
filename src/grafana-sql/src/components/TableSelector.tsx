// import { useAsync } from 'react-use';

import { SelectableValue, toOption } from '@grafana/data';
import { selectors } from '@grafana/e2e-selectors';
import { Select } from '@grafana/ui';
import { useState, useEffect, useCallback } from 'react';
// import { debounce } from 'lodash'; // You can use lodash.debounce for debouncing

import { DB, ResourceSelectorProps } from '../types';

export interface TableSelectorProps extends ResourceSelectorProps {
  db: DB;
  table: string | undefined;
  dataset: string | undefined;
  onChange: (v: SelectableValue) => void;
  inputId?: string | undefined;
}

export const TableSelector = ({ db, dataset, table, className, onChange, inputId }: TableSelectorProps) => {
  const [inputValue, setInputValue] = useState(table || ''); // Track user input
  const [options, setOptions] = useState([] as SelectableValue[]); // Options for the Select component
  const [loading, setLoading] = useState(false); // Loading state for the Select component

  // Fetch tables from the API based on input value
  const fetchTables = useCallback(async (input: string) => {
    debugger;
    if (!dataset) {
      debugger;
      setOptions([]);
      return;
    }

    setLoading(true);

    try {
      const tables = await db.tables(dataset, input); // Fetch tables based on input value
      setOptions(tables.map(toOption)); // Map the result to options for Select
    } catch (error) {
      console.error('Error fetching tables:', error);
    } finally {
      setLoading(false);
    }
  }, [db, dataset]);

  // Handle user input changes
  const handleInputChange = (newInput: string) => {
    setInputValue(newInput); // Update the input value as the user types
  };

  // Handle "Enter" key press to submit the table name
  const handleKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === 'Enter') {
      // If the user presses "Enter", trigger the API call
      if (inputValue.includes('*')) {
        fetchTables(inputValue); // Fetch tables with the input value if '*' is included
      } else {
        debugger;
        // Optionally, you can handle selecting a table without '*'
        setOptions([]); // Reset options if the input doesn't contain '*'
      }
    }
  };

  // Trigger the API call when the component first loads, if inputValue contains '*'
  useEffect(() => {
    fetchTables(inputValue); // Fetch tables immediately if input contains '*'
  }, [dataset]); // This runs only once on initial render

  useEffect(() => {
    // Trigger the API call if the table name is updated from the parent component.
    if (table && table !== inputValue && inputValue != "") {
      setInputValue(table); // Update input value if the parent passed a new table
      if (table.includes('*')) {
        fetchTables(table); // Fetch tables immediately if table contains '*'
      } else {
        // debugger;
        // setOptions([]); // Reset options if table doesn't contain '*'
      }
    }
  }, [table]); // This effect listens for changes in the `table` prop
  // // Debounced function to fetch tables when input changes and contains '*'
  // const debouncedFetchTables = useCallback(
  //   debounce(() => {
  //     if (inputValue.includes('*')) {
  //       fetchTables(); // Fetch new tables if input contains '*'
  //     }
  //   }, 300), // 300ms debounce
  //   [inputValue, fetchTables]
  // );

  // useEffect(() => {
  //   if (inputValue.includes('*')) {
  //     debouncedFetchTables(); // Trigger API call if '*' is in the input value
  //   }
  // }, [inputValue, debouncedFetchTables]);

  // const handleInputChange = (newInput: string) => {
  //   setInputValue(newInput); // Update the input value
  //   if (!newInput.includes('*')) {
  //     fetchTables(); // Fetch tables immediately if input doesn't contain '*'
  //   }
  // };


  // const state = useAsync(async () => {
  //   // No need to attempt to fetch tables for an unknown dataset.
  //   if (!dataset) {
  //     return [];
  //   }

  //   const tables = await db.tables(dataset, table);
  //   return tables.map(toOption);
  // }, [dataset, table]);

  return (
    <Select
      className={className}
      disabled={loading}
      aria-label="Table selector"
      inputId={inputId}
      data-testid={selectors.components.SQLQueryEditor.headerTableSelector}
      value={table}
      options={options}
      onChange={onChange}
      isLoading={loading}
      menuShouldPortal={true}
      placeholder={loading ? 'Loading tables' : 'Select table'}
      onInputChange={handleInputChange} // Update input value as the user types
      onKeyDown={handleKeyDown} // Listen for Enter key press to trigger the API call
    />
  );
};
