from enum import Enum
from datetime import datetime, timedelta
from threading import Lock


class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """
    Circuit Breaker simple para proteger llamadas HTTP inestables.

    Estados:
    - CLOSED:
        Todo funciona normalmente.
        Se permiten requests.
    
    - OPEN:
        La API falló demasiadas veces.
        Se bloquean requests temporalmente.
    
    - HALF_OPEN:
        Se prueba una request de recuperación.
        Si funciona -> CLOSED
        Si falla -> OPEN
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 30,
        success_threshold: int = 2
    ):
        """
        Args:
            failure_threshold:
                Cantidad máxima de errores consecutivos
                antes de abrir el circuito.

            recovery_timeout:
                Tiempo (segundos) antes de intentar
                pasar a HALF_OPEN.

            success_threshold:
                Cantidad de éxitos necesarios en HALF_OPEN
                para volver a CLOSED.
        """

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        self.state = CircuitState.CLOSED

        self.failure_count = 0
        self.success_count = 0

        self.last_failure_time = None

        self._lock = Lock()

    # =========================================================
    # MÉTODO PRINCIPAL
    # =========================================================

    def call(self, func, *args, **kwargs):
        """
        Ejecuta una función protegida por Circuit Breaker.
        """

        with self._lock:

            # =================================================
            # OPEN -> verificar si puede pasar a HALF_OPEN
            # =================================================

            if self.state == CircuitState.OPEN:

                if self._can_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    print("[CircuitBreaker] OPEN -> HALF_OPEN")
                else:
                    raise Exception(
                        "Circuit OPEN: requests temporalmente bloqueadas."
                    )

        # =====================================================
        # Ejecutar request protegida
        # =====================================================

        try:
            result = func(*args, **kwargs)

        except Exception as e:

            self._record_failure()

            raise Exception(
                f"[CircuitBreaker] Request falló: {str(e)}"
            )

        # =====================================================
        # Request exitosa
        # =====================================================

        self._record_success()

        return result

    # =========================================================
    # MANEJO DE FALLAS
    # =========================================================

    def _record_failure(self):

        with self._lock:

            self.failure_count += 1
            self.last_failure_time = datetime.utcnow()

            print(
                f"[CircuitBreaker] Failure count: {self.failure_count}"
            )

            # ================================================
            # Si estamos en HALF_OPEN y falla:
            # volver inmediatamente a OPEN
            # ================================================

            if self.state == CircuitState.HALF_OPEN:

                self.state = CircuitState.OPEN
                self.success_count = 0

                print("[CircuitBreaker] HALF_OPEN -> OPEN")

                return

            # ================================================
            # Si supera umbral -> OPEN
            # ================================================

            if self.failure_count >= self.failure_threshold:

                self.state = CircuitState.OPEN

                print("[CircuitBreaker] CLOSED -> OPEN")

    # =========================================================
    # MANEJO DE ÉXITOS
    # =========================================================

    def _record_success(self):

        with self._lock:

            # ================================================
            # HALF_OPEN
            # ================================================

            if self.state == CircuitState.HALF_OPEN:

                self.success_count += 1

                print(
                    f"[CircuitBreaker] HALF_OPEN success: "
                    f"{self.success_count}"
                )

                if self.success_count >= self.success_threshold:

                    self._reset()

                    print("[CircuitBreaker] HALF_OPEN -> CLOSED")

            # ================================================
            # CLOSED
            # ================================================

            else:

                self.failure_count = 0

    # =========================================================
    # RESET
    # =========================================================

    def _reset(self):

        self.state = CircuitState.CLOSED

        self.failure_count = 0
        self.success_count = 0

        self.last_failure_time = None

    # =========================================================
    # RECOVERY TIMER
    # =========================================================

    def _can_attempt_reset(self) -> bool:

        if not self.last_failure_time:
            return False

        elapsed_time = (
            datetime.utcnow() - self.last_failure_time
        )

        return elapsed_time >= timedelta(
            seconds=self.recovery_timeout
        )

    # =========================================================
    # DEBUG / STATUS
    # =========================================================

    def get_status(self):

        return {
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": (
                self.last_failure_time.isoformat()
                if self.last_failure_time
                else None
            )
        }


# =============================================================
# EJEMPLO DE USO
# =============================================================

if __name__ == "__main__":

    import random
    import time

    breaker = CircuitBreaker(
        failure_threshold=3,
        recovery_timeout=10,
        success_threshold=2
    )

    def unstable_api():

        value = random.random()

        # 70% falla
        if value < 0.7:
            raise Exception("503 Service Unavailable")

        return {"status": "ok"}

    for i in range(20):

        print(f"\nRequest #{i+1}")

        try:

            response = breaker.call(unstable_api)

            print("SUCCESS:", response)

        except Exception as e:

            print("ERROR:", e)

        print("STATUS:", breaker.get_status())

        time.sleep(1)
