from stdflow.stdflow_utils import remove_dir


def test_remove_dir():
    assert "./fr/data/coucou/ui/coucou/ok/" == remove_dir("./fr/data/coucou/ui/coucou/ok/ui/", "ui")

    assert "./fr/data/coucou/ui/coucou/ok/" == remove_dir("./fr/data/coucou/ui/coucou/ok/ui/", "ui")

    assert "./fr/data/coucou/ui/coucou/ok" == remove_dir("./fr/data/coucou/ui/coucou/ok/ui", "ui")

    assert "./fr/data/coucou/ui/coucou/ui" == remove_dir("./fr/data/coucou/ui/coucou/ok/ui", "ok")

    assert "./fr/data/coucou/ui/ok/ui" == remove_dir("./fr/data/coucou/ui/coucou/ok/ui", "coucou")

    assert "./fr/data/ui/coucou_/ok/ui" == remove_dir("./fr/data/coucou/ui/coucou_/ok/ui", "coucou")
