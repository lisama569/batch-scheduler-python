"""冒烟测试骨架。"""


def test_imports() -> None:
    from batch_scheduler.models import Resources
    from batch_scheduler.scheduler import Scheduler
    from batch_scheduler.service import create_app

    s = Scheduler(Resources(cpu=1), {})
    app = create_app(s)
    assert app.title
