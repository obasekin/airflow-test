import json
import logging
from typing import Any

from citadel.jupyter.jupyter_executor import (
    DEFAULT_CONN_ID,
    DEFAULT_TIMEOUT,
    _session,
    _select_kernel,
    _execute_in_kernel,
    _save_notebook,
    _as_notebook_output,
    get_jupyterhub_config,
)

logger = logging.getLogger(__name__)


def _get_notebook(notebook_path: str, conn_id: str) -> dict[str, Any]:
    config = get_jupyterhub_config(conn_id)
    contents_url = f"{config['base_url']}/user/{config['username']}/api/contents"
    response = _session(conn_id).get(
        f"{contents_url}/{notebook_path}",
        timeout=DEFAULT_TIMEOUT,
        allow_redirects=False,
    )
    response.raise_for_status()
    return response.json()


def run_existing_notebook(
    notebook_path: str,
    conn_id: str = DEFAULT_CONN_ID,
    timeout: int = DEFAULT_TIMEOUT,
    fail_on_execution_error: bool = True,
) -> dict[str, Any]:
    """Fetch an existing notebook, execute its code cells, save outputs, and return results."""
    
    # 1. Fetch existing notebook
    notebook_model = _get_notebook(notebook_path, conn_id)
    if notebook_model.get("type") != "notebook":
        raise ValueError(f"Path '{notebook_path}' is not a notebook.")
        
    notebook = notebook_model.get("content", {})
    cells = notebook.get("cells", [])
    
    # 2. Select or create a kernel
    kernel_id = _select_kernel(conn_id)
    
    all_outputs = []
    overall_status = "success"
    
    # 3. Execute cells one by one
    exec_count = 1
    for cell in cells:
        if cell.get("cell_type") != "code":
            continue
            
        source = cell.get("source", "")
        if isinstance(source, list):
            code = "".join(source)
        else:
            code = source
            
        if not code.strip():
            continue
            
        execution = _execute_in_kernel(code, kernel_id, conn_id, timeout)
        
        cell["execution_count"] = exec_count
        exec_count += 1
        
        cell["outputs"] = [
            _as_notebook_output(output)
            for output in execution["outputs"]
        ]
        
        all_outputs.extend(execution["outputs"])
        
        if execution["execution_status"] != "ok":
            overall_status = "failed"
            if fail_on_execution_error:
                break
                
    # 4. Save the updated notebook back to JupyterHub
    saved_info = _save_notebook(notebook_path, notebook_model, conn_id)
    
    result = {
        "status": overall_status,
        **saved_info,
        "kernel_id": kernel_id,
        "outputs": all_outputs,
    }
    
    logger.info("Executed existing notebook. URL: %s", result.get("notebook_url"))
    
    if fail_on_execution_error and overall_status == "failed":
        error_outputs = [o for o in all_outputs if o.get("type") == "error"]
        raise RuntimeError(
            f"Existing Notebook execution failed. "
            f"Notebook URL: {result.get('notebook_url')}. "
            f"Errors: {json.dumps(error_outputs, ensure_ascii=False)}"
        )
        
    return result
