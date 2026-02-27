# Metaflow plugin registration for metaflow-akash.
#
# Layer: Plugin Registration (top-level entry point)
# May only import from: .akash_decorator, .akash_cli

STEP_DECORATORS_DESC = [
    ("akash", ".akash_decorator.AkashDecorator"),
]

CLIS_DESC = [
    ("akash", ".akash_cli.cli"),
]
