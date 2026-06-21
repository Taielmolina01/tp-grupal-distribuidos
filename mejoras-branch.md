# Mejoras habladas con Pablo

- UDP no justifica en el bully deberia ser TCP, es más overhead pero son pocos nodos.
- Deberia tener hilos listener and sender.
- Cada watchdog deberia solo pingear en el caso de que sea líder, si no lo es no hacerlo. Para ello necesito tener una condvar donde _sleepen_ los watchdog hasta que se cumpla que son lideres. 
- Stoppear el container antes de restartearlo por las dudas.
