from dags.drafts.great_exp.gx_manager import GxManager


def main():
    # #  define AWS_PROFILE='m-d-l-dev' in ENV
    env = "nonprod"
    # source = "ld_clicked_item_position"
    source = "zuora_aqua_obj_delta_account"
    # source = "ares_vimond_player_log"
    #  ## ^ To be passed as script arguments ##

    gx_manager = GxManager(env, source, engine="athena")
    gx_manager.exec()
    gx_manager.build_data_docs()


if __name__ == "__main__":
    main()
