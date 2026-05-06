"""
(4) Cliente gRPC
  - Consulta a leitura mais recente
  - Consulta histórico da última hora (streaming)
"""

import time

import grpc

import temperature_pb2
import temperature_pb2_grpc

from const import DEADLINE_SECONDS, GRPC_SERVER


def run():
    with grpc.insecure_channel(GRPC_SERVER) as channel:
        stub = temperature_pb2_grpc.TemperatureServiceStub(channel)

        print("=== Última leitura processada ===")
        try:
            response = stub.GetLatest(
                temperature_pb2.GetLatestRequest(),
                timeout=DEADLINE_SECONDS,
            )
            print(f"  Temperatura média : {response.average_celsius:.2f} °C")
            print(f"  Amostras na janela: {response.sample_count}")
            print(f"  Timestamp         : {response.timestamp}")
        except grpc.RpcError as e:
            print(f"  Erro ({e.code().name}): {e.details()}")

        print("\n=== Histórico — última hora (stream) ===")
        now = int(time.time())
        one_hour_ago = now - 3600
        try:
            stream = stub.GetHistory(
                temperature_pb2.GetHistoryRequest(
                    start_timestamp=one_hour_ago,
                    end_timestamp=now,
                ),
                timeout=DEADLINE_SECONDS,
            )
            count = 0
            for reading in stream:
                print(
                    f"  [{reading.timestamp}] "
                    f"avg={reading.average_celsius:.2f} °C  "
                    f"amostras={reading.sample_count}"
                )
                count += 1
            if count == 0:
                print("  Nenhum registro encontrado no período.")
            else:
                print(f"  Total: {count} registros.")
        except grpc.RpcError as e:
            print(f"  Erro ({e.code().name}): {e.details()}")


if __name__ == "__main__":
    run()
