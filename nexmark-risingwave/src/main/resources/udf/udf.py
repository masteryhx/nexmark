# Import components from the risingwave.udf module
from risingwave.udf import udf, UdfServer

@udf(input_types=['VARCHAR', 'VARCHAR'], result_type='BIGINT')
def count_char(s, char):
    count = 0
    if s is not None:
      for c in s:
         if c == char:
             count += 1
    return count

# Start a UDF server
if __name__ == '__main__':
    server = UdfServer(location="0.0.0.0:8815") # You can use any available port in your system. Here we use port 8815.
    server.add_function(count_char)
    server.serve()