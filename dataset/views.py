import pandas as pd
from rest_framework.decorators import api_view
from rest_framework.response import Response

# cache temporal
DATAFRAME_CACHE = {}

@api_view(['POST'])
def upload_dataset(request):
    file = request.FILES["file"]
    df = pd.read_csv(file)
    DATAFRAME_CACHE["df"] = df
    return Response({"message": "Archivo cargado", "rows": len(df)})

@api_view(['GET'])
def head(request):
    df = DATAFRAME_CACHE.get("df")
    if df is None:
        return Response({"error": "No dataset cargado"}, status=400)
    return Response(df.head().to_dict(orient="records"))

@api_view(['GET'])
def missing(request):
    df = DATAFRAME_CACHE.get("df")
    if df is None:
        return Response({"error": "No dataset cargado"}, status=400)
    missing = df.isnull().sum().to_dict()
    return Response(missing)

@api_view(['GET'])
def duplicates(request):
    df = DATAFRAME_CACHE.get("df")
    if df is None:
        return Response({"error": "No dataset cargado"}, status=400)
    duplicates = df.duplicated().sum()
    total = len(df)
    return Response({
        "duplicadas": int(duplicates),
        "unicas": int(total - duplicates)
    })


@api_view(['GET'])
def dtypes(request):
    df = DATAFRAME_CACHE.get("df")
    if df is None:
        return Response({"error": "No dataset cargado"}, status=400)
    dtypes_count = df.dtypes.value_counts().to_dict()
    # Convertir tipos de pandas a str
    dtypes_count = {str(k): int(v) for k, v in dtypes_count.items()}
    return Response(dtypes_count)


@api_view(['GET'])
def unique_counts(request):
    df = DATAFRAME_CACHE.get("df")
    if df is None:
        return Response({"error": "No dataset cargado"}, status=400)
    unique_counts = df.nunique().to_dict()
    return Response(unique_counts)
