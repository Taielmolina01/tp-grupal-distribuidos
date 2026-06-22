package priorityqueue

const (
	_GROWTH_FACTOR    int    = 2
	_REDUCTION_FACTOR int    = 2
	_MIN_TRESHOLD     int    = 4
	_INITIAL_LEN      int    = 70
	_PANIC_MSG        string = "The queue is empty"
)

type heap[T comparable] struct {
	len        int
	datos      []T
	comparador func(T, T) int
}

// Crea un heap vacío, guardando la función de comparación correspondiente.
func NewHeap[T comparable](funcion_cmp func(T, T) int) PriorityQueue[T] {
	heap := new(heap[T])
	heap.datos = make([]T, _INITIAL_LEN)
	heap.comparador = funcion_cmp
	return heap
}

func (heap *heap[T]) IsEmpty() bool {
	return heap.len == 0
}

func (heap *heap[T]) Enqueue(dato T) {
	heap.redimensionarSiSeRequiere()
	heap.datos[heap.len] = dato
	upHeap(heap.datos, heap.comparador, heap.len)
	heap.len++
}

func (heap *heap[T]) GetMax() T {
	if heap.IsEmpty() {
		panic(_PANIC_MSG)
	}
	return heap.datos[0]
}

func (heap *heap[T]) Dequeue() T {
	if heap.IsEmpty() {
		panic(_PANIC_MSG)
	}
	datoADevolver := heap.datos[0]
	heap.len--
	heap.redimensionarSiSeRequiere()
	swap(heap.datos, 0, heap.len)
	downHeap(heap.datos, heap.comparador, 0, heap.len)
	return datoADevolver
}

func (heap *heap[T]) Update(oldVal, newVal T) {
	pos := -1
	// puede hacerse O(log n) con un mapa pero se me va un poco de scope para el tp.
	for i := 0; i < heap.len; i++ {
		if heap.datos[i] == oldVal {
			pos = i
			break
		}
	}
	if pos == -1 {
		return
	}
	heap.datos[pos] = newVal
	cmp := heap.comparador(newVal, oldVal)
	if cmp > 0 {
		upHeap(heap.datos, heap.comparador, pos)
	} else if cmp < 0 {
		downHeap(heap.datos, heap.comparador, pos, heap.len)
	}
}

func (heap *heap[T]) Len() int {
	return heap.len
}

// Dado un array, lo convierte en un heap, utilizando el algoritmo Heapify.
func CrearHeapArr[T comparable](arreglo []T, funcion_cmp func(T, T) int) PriorityQueue[T] {
	if len(arreglo) == 0 {
		return NewHeap(funcion_cmp)
	} else {
		heap := new(heap[T])
		heap.len = len(arreglo)
		heap.datos = make([]T, max(heap.len*_GROWTH_FACTOR, _INITIAL_LEN))
		copy(heap.datos, arreglo)
		heap.comparador = funcion_cmp
		heapify(heap.datos, heap.comparador, heap.len)
		return heap
	}
}

// Realiza el algoritmo Heapify, que es realizar downHeap del último al primero, para convertir un arreglo a un heap.
func heapify[T any](arr []T, funcion_cmp func(T, T) int, len int) {
	for i := (len / 2) - 1; i >= 0; i-- {
		downHeap(arr, funcion_cmp, i, len)
	}
}

// Ejecuta el algoritmo de ordenamiento HeapSort, que consiste en realizar Heapify y luego ir desencolando
// del heap (que es el valor máximo) y ubicandolo al final del arreglo.
// El orden de ejecucion es (n log n) siendo n la cantidad de elementos del array.
func HeapSort[T comparable](elementos []T, funcion_cmp func(T, T) int) {
	heap := new(heap[T])
	heap.datos = elementos
	heap.len = len(heap.datos)
	heap.comparador = funcion_cmp
	heapify(heap.datos, heap.comparador, heap.len)

	numeroIteraciones := heap.len
	for i := 0; i < numeroIteraciones; i++ {
		elementos[heap.len] = heap.Dequeue()
	}
}

// Funciones/Primitivas auxiliares

// Evalua si se requiere una redimensión del arreglo del heap, ya sea agrandandolo o achicandolo.
func (heap *heap[T]) redimensionarSiSeRequiere() {
	if heap.len > cap(heap.datos)/(_REDUCTION_FACTOR*_MIN_TRESHOLD) {
		if heap.len == cap(heap.datos) {
			arrayRedimensionado := make([]T, cap(heap.datos)*_GROWTH_FACTOR)
			copy(arrayRedimensionado, heap.datos)
			heap.datos = arrayRedimensionado
		}
		if heap.len*_MIN_TRESHOLD <= cap(heap.datos) {
			arrayRedimensionado := make([]T, cap(heap.datos)/_REDUCTION_FACTOR)
			copy(arrayRedimensionado, heap.datos)
			heap.datos = arrayRedimensionado
		}
	}
}

// Helpers

// Realiza el algoritmo UpHeap, que se basa en comparar un  hijo con su padre, y chequear que el hijo
// sea menor que su padre; caso contrario, swapea los datos.
func upHeap[T any](arr []T, compareFunc func(T, T) int, posActual int) {
	posPadre := modulo((posActual - 1) / 2)

	if posPadre == posActual || compareFunc(arr[posActual], arr[posPadre]) <= 0 {
		return
	}
	swap(arr, posActual, posPadre)
	upHeap(arr, compareFunc, posPadre)
}

// Realiza el algoritmo DownHeap, que se basa en comparar un  hijo con su padre, y chequear que el hijo
// sea menor que su padre; caso contrario, swapea los datos.
func downHeap[T any](arr []T, compareFunc func(T, T) int, posActual int, len int) {
	posHijoIzq := 2*posActual + 1
	posHijoDer := 2*posActual + 2
	var posMayor int

	if posHijoDer < len {
		posMayor = maxPos(arr, compareFunc, posHijoIzq, posHijoDer)
		if compareFunc(arr[posMayor], arr[posActual]) > 0 {
			swap(arr, posActual, posMayor)
			downHeap(arr, compareFunc, posMayor, len)
		}
	}
	if posHijoIzq < len {
		posMayor = posHijoIzq
		if compareFunc(arr[posMayor], arr[posActual]) > 0 {
			swap(arr, posActual, posMayor)
			downHeap(arr, compareFunc, posMayor, len)
		}
	}
}

// Devuelve el módulo de un número.
func modulo(num int) int {
	if num < 0 {
		return -num
	}
	return num
}

// Dado dos posicion del heap, swapea sus valores en el array interno.
func swap[T any](arr []T, pos1, pos2 int) {
	arr[pos1], arr[pos2] = arr[pos2], arr[pos1]
}

// Dadas dos posiciones devuelve el máximo entre los dos valores, correspondientes a las posiciones dadas en el heap.
func maxPos[T any](arr []T, compareFunc func(T, T) int, pos1, pos2 int) int {
	if compareFunc(arr[pos1], arr[pos2]) > 0 {
		return pos1
	}
	return pos2
}
