from processes.target_data import target_data
from processes.update_m_billing_destinations import update_m_billing_destinations
from processes.create_m_clients import create_m_clients
from processes.create_m_mechanics import create_m_mechanics
from processes.create_m_vehicle_manufacturers import create_m_vehicle_manufacturers
from processes.create_m_vehicle_models import create_m_vehicle_models
from processes.create_m_scds import create_m_scds


def lambda_handler():
    errors = []
    taget_ids = target_data().get_t_matching_data()
    for item in taget_ids:
        error = []
        t_matching_data_id = item['id']
        print(t_matching_data_id)

        if(target_data().is_t_repair_data_by_m_matching_id_exist(t_matching_data_id) is False):
            continue

        result = update_m_billing_destinations().execute(t_matching_data_id)
        if(result is False):
            error.append(
                f"[ERROR] in update_billing_destinations. t_matching_data_id : {t_matching_data_id}. {result}")

        result = create_m_clients().execute(t_matching_data_id)
        if(result is False):
            error.append(
                f"[ERROR] in create_m_clients. t_matching_data_id : {t_matching_data_id}. {result}")

        result = create_m_mechanics().execute(t_matching_data_id)
        if(result is False):
            error.append(
                f"[ERROR] in create_m_mechanics. t_matching_data_id : {t_matching_data_id}. {result}")

        result = create_m_vehicle_manufacturers().execute(t_matching_data_id)
        if(result is False):
            error.append(
                f"[ERROR] in create_m_vehicle_manufacturers. t_matching_data_id : {t_matching_data_id}. {result}")

        result = create_m_vehicle_models().execute(t_matching_data_id)
        if (result is False):
            error.append(
                f"[ERROR] in create_m_vehicle_models. t_matching_data_id : {t_matching_data_id}. {result}")

        result = create_m_scds().execute(t_matching_data_id)
        if (result is False):
            error.append(
                f"[ERROR] in create_m_scds. t_matching_data_id : {t_matching_data_id}. {result}")

        if not error:
            result = target_data().update_is_output_master_to_true(t_matching_data_id)
            if (result is False):
                error.append(
                    f"[ERROR] in update_is_output_master_to_true. t_matching_data_id : {t_matching_data_id}. {result}")

        errors.append(error)

    if errors:
        # エラーが発生した場合のアクション
        print(errors)


lambda_handler()
