"""Configuração da suíte de testes.

Coloca a raiz do repositório no `sys.path` para que `from src import ...`
funcione, e desliga a gravação de bytecode: o filesystem do Databricks Workspace
não permite criar diretórios `__pycache__`, e o Python falharia com
`OSError [Errno 95]` antes de executar qualquer teste.
"""

import sys
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
