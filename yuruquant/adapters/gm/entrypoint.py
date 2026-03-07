from yuruquant.app.runtime import dispatch_callback


def init(context):
    return dispatch_callback('init', context)


def on_bar(context, bars):
    return dispatch_callback('on_bar', context, bars)


def on_order_status(context, order):
    return dispatch_callback('on_order_status', context, order)


def on_execution_report(context, execrpt):
    return dispatch_callback('on_execution_report', context, execrpt)


def on_error(context, code, info_msg):
    return dispatch_callback('on_error', context, code, info_msg)
