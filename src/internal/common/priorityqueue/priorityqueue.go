package priorityqueue

type PriorityQueue[T comparable] interface {

	// EstaVacia devuelve true si la cantidad de elementos en el heap es 0, false en caso contrario. */
	IsEmpty() bool

	// Encolar Agrega un elemento al heap.
	Enqueue(T)

	// VerMax devuelve el elemento con máxima prioridad. Si está vacía, entra en pánico con un mensaje
	// "La cola está vacia".
	GetMax() T

	// Desencolar elimina el elemento con máxima prioridad, y lo devuelve. Si está vacía, entra en pánico con un
	// mensaje "El heap esta vacia"
	Dequeue() T

	// Cantidad devuelve la cantidad de elementos que hay en la cola de prioridad.
	Len() int
}
