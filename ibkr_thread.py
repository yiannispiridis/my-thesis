import logging
import threading


def run_ib_api(app):
    """Runs the IB API event loop in a separate thread."""
    try:
        app.run()
    except Exception as e:
        logging.error(f"IB API run error: {e}")
    finally:
        logging.info("IB API loop stopped.")


def start_api_thread(app):
    """Starts the IB API in a background thread to keep it responsive."""
    api_thread = threading.Thread(target=run_ib_api, args=(app,), daemon=True)
    api_thread.start()
    return api_thread