from jinja2 import Environment, FileSystemLoader


class Templater:
    def __init__(self, path_to_templates: str) -> None:
        # FIXME: Docstring
        self.path_to_templates = path_to_templates
        self.engine = self._load()

    def _load(self):
        # FIXME: Docstring
        return Environment(loader=FileSystemLoader(self.path_to_templates))

    def render_template(self, template_name: str, **kwargs):
        # FIXME: Docstring
        return self.engine.get_template(template_name).render(**kwargs)
