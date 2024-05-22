from mpi4py import MPI

class TablaHash:
    def __init__(self, size):
        self.size = size
        self.table = {}

    def _hash(self, key):
        return hash(key) % self.size

    def insert(self, key, value):
        index = self._hash(key)
        if index not in self.table:
            self.table[index] = [(key, value)]
        else:
            self.table[index].append((key, value))
        print(f"Proceso {MPI.COMM_WORLD.Get_rank()}: Clave: {key} insertada con valor: {value}")

    def search(self, key):
        index = self._hash(key)
        if index in self.table:
            for k, v in self.table[index]:
                if k == key:
                    print(f"Proceso {MPI.COMM_WORLD.Get_rank()}: Clave: {key} encontrada con valor: {v}")
                    return v
        print(f"Proceso {MPI.COMM_WORLD.Get_rank()}: Clave {key} no encontrada")
        return None

# Inicialización de MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Crear la tabla hash en cada proceso
hash_table = TablaHash(150000)

# Definir y distribuir los datos a insertar entre los procesos
if rank == 0:
    data_to_insert = [(f"key{i}", f"value{i}") for i in range(150000)]
    # Dividir los datos en partes iguales para cada proceso
    chunk_size = len(data_to_insert) // size
    data_chunks = [data_to_insert[i * chunk_size:(i + 1) * chunk_size] for i in range(size)]
else:
    data_chunks = None

# Distribuir los datos entre los procesos
data_to_insert = comm.scatter(data_chunks, root=0)

# Cada proceso inserta su subconjunto de datos en la tabla hash
for key, value in data_to_insert:
    hash_table.insert(key, value)

# Recopilar las tablas hash de todos los procesos en el proceso 0
all_tables = comm.gather(hash_table.table, root=0)

if rank == 0:
    # Fusionar todas las tablas hash
    combined_table = {}
    for table in all_tables:
        for index, key_values in table.items():
            if index not in combined_table:
                combined_table[index] = key_values
            else:
                combined_table[index].extend(key_values)
    
    # Crear una nueva tabla hash combinada
    combined_hash_table = TablaHash(150000)
    combined_hash_table.table = combined_table
else:
    combined_hash_table = None

# Definir y distribuir las claves a buscar entre los procesos
if rank == 0:
    keys_to_search = [f"key{i}" for i in range(5000, 5050)]
    # Dividir las claves en partes iguales para cada proceso
    chunk_size = len(keys_to_search) // size
    keys_chunks = [keys_to_search[i * chunk_size:(i + 1) * chunk_size] for i in range(size)]
else:
    keys_chunks = None

# Distribuir las claves entre los procesos
keys_to_search = comm.scatter(keys_chunks, root=0)

# Cada proceso busca su subconjunto de claves en la tabla hash combinada
search_results = []
for key in keys_to_search:
    if combined_hash_table is not None:
        result = combined_hash_table.search(key)
        search_results.append((key, result))

# Recopilar los resultados de búsqueda en el proceso 0
all_results = comm.gather(search_results, root=0)