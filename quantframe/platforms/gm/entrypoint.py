from quantframe.app.runtime import dispatch_callback


def init(context):
    return dispatch_callback("initialize", context)


def on_bar(context, bars):
    return dispatch_callback("on_bar", context, list(bars or []))


def on_order_status(context, order):
    return dispatch_callback("on_order_status", context, order)


def on_execution_report(context, execution):
    return dispatch_callback("on_execution_report", context, execution)


def on_error(context, code, info):
    return dispatch_callback("on_error", context, code, info)
