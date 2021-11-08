"""
Script to get sum of sales for all dates
"""
import csv

# import dask
# import dask.dataframe as dd


class SumOfSales:
    """
    Calculates the sale sum of all departments
    """
    input_csv = "data/input.csv"
    output_csv = "data/output.csv"

    # def with_dask(self):
    #     """
    #     This is a reference method to process large CSVs with dask dataframes; not implemented.
    #     :return:
    #     """
    #
    #     df = dd.read_csv(self.input_csv)
    #
    #     @dask.delayed
    #     def print_a_block(d):
    #         for row in df:
    #             print(row)
    #
    #     dask.compute(*[print_a_block(d) for d in df.to_delayed()])

    def main(self):
        """
        Main method to start the process
        :return: None
        """
        output = dict()
        data = csv.DictReader(open(self.input_csv))
        for row in data:
            department = row.get('department', '')
            if department in output:
                output[department] += int(row.get('sale', 0))
            else:
                output[department] = int(row.get('sale', 0))
        self.generate_output_csv(output)

    def generate_output_csv(self, data):
        """
        Writes the output CSV
        :param dict data: dictionary of the total sales
        :return: None
        """
        if data is None:
            data = {}
        try:
            with open(self.output_csv, mode='w+') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(['Department', 'Sales'])
                for key, value in data.items():
                    writer.writerow([key, value])
        except Exception as e:
            print(f'Exception while writing csv - {e}')

if __name__ == '__main__':
    csv_reader = SumOfSales()
    csv_reader.main()
