from airflow.models.baseoperator import BaseOperator
from airflow.models.xcom import XCom
from airflow.utils.session import provide_session
from airflow.utils.trigger_rule import TriggerRule


class CleanupXComOperator(BaseOperator):
    """
    Cleans XCom entries for the current DAG run only.

    - Scoped by dag_id and run_id
    - Safe if no XCom exists
    - Intended as a terminal cleanup task
    """

    def __init__(
        self,
        *,
        trigger_rule: str = TriggerRule.ALL_DONE,
        **kwargs,
    ):
        super().__init__(trigger_rule=trigger_rule, **kwargs)

    @provide_session
    def execute(self, context, session=None):
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]

        deleted = (
            session.query(XCom)
            .filter(
                XCom.dag_id == dag_id,
                XCom.run_id == run_id,
            )
            .delete(synchronize_session=False)
        )

        session.commit()

        self.log.info(
            "Deleted %s XCom records for dag_id=%s, run_id=%s",
            deleted,
            dag_id,
            run_id,
        )
